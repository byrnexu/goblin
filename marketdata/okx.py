"""
OKX交易所市场数据实现模块

本模块实现了OKX交易所的市场数据订阅功能，支持：
1. 现货市场
2. USDT本位永续合约
3. 币本位永续合约

主要功能：
- 订单簿（orderbook）订阅与快照
- 逐笔成交（trade）订阅
- 自动重连机制
- 订单簿快照同步
- 合约类型切换

工作流程：
1. 建立WebSocket连接
2. 订阅指定交易对的数据
3. 接收并处理实时数据
4. 维护本地订单簿状态
5. 通知订阅者数据更新

注意事项：
- 订单簿数据需要先获取快照，再通过增量更新维护
- 需要处理订单簿的序号同步问题
- 不同市场类型使用不同的WebSocket和REST API地址
"""

import json
import asyncio
from typing import Dict, List, Optional, Callable, Union, Awaitable, Any
from decimal import Decimal
import websockets
import aiohttp
from .base import MarketDataBase, OrderBook, OrderBookLevel, Trade
from .config import OkxConfig
from .types import MarketType
from sortedcontainers import SortedDict
from util.logger import get_logger
from util.symbol_convert import to_exchange, from_exchange
from util.websocket_manager import WebSocketManager

class OkxMarketData(MarketDataBase):
    """
    OKX永续合约市场数据实现（USDT本位和币本位）
    支持：
    - 订单簿（orderbook）订阅与快照
    - 逐笔成交（trade）订阅
    - 自动重连、快照同步、合约类型切换

    工作流程：
    1. 初始化时设置市场类型和配置
    2. 连接时建立WebSocket和REST连接
    3. 订阅数据时发送订阅请求
    4. 接收数据时更新本地状态并通知订阅者
    5. 断开连接时清理资源

    订单簿处理流程：
    1. 获取初始快照
    2. 接收增量更新
    3. 验证更新序号
    4. 合并更新到本地状态
    5. 通知订阅者

    成交数据处理流程：
    1. 接收成交消息
    2. 转换为内部格式
    3. 通知订阅者
    """
    def __init__(self, config: OkxConfig = OkxConfig(), market_type: MarketType = MarketType.SPOT):
        """
        初始化市场数据对象

        Args:
            config: OKX配置对象，包含API地址、深度限制等配置
            market_type: 市场类型，使用 MarketType 枚举

        初始化过程：
        1. 调用父类初始化
        2. 设置市场类型相关配置
        3. 初始化WebSocket和HTTP会话
        4. 设置请求ID计数器
        """
        super().__init__(config, market_type)
        self.logger.info(f"初始化OKX{market_type.value}市场数据服务...")

        # 设置订单簿深度限制和更新间隔
        self._orderbook_depth_limit: int = config.ORDERBOOK_DEPTH_LIMIT[market_type.value]
        self._orderbook_update_interval: str = config.ORDERBOOK_UPDATE_INTERVAL[market_type.value]

        # 设置REST API地址
        self._rest_url: str = config.REST_URLS[market_type.value]
        # HTTP会话对象，用于REST API请求
        self._session: Optional[aiohttp.ClientSession] = None
        # WebSocket请求ID计数器
        self._next_request_id: int = 1
        # 存储订阅请求内容，用于重连时重新订阅
        self._subscription_requests: Dict[int, Dict[str, Any]] = {}

    def get_ws_url(self) -> str:
        """获取WebSocket URL
        
        OKX使用单一的WebSocket URL，而不是按市场类型区分
        
        Returns:
            str: OKX WebSocket URL
        """
        return self._config.WS_URL

    async def connect(self) -> None:
        """
        建立WebSocket和REST连接，并启动消息处理循环

        连接过程：
        1. 创建HTTP会话
        2. 建立WebSocket连接
        3. 设置运行状态
        4. 记录连接日志
        """
        self.logger.info("开始建立市场数据连接...")
        self._session = aiohttp.ClientSession()

        await self._ws_manager.connect()

        self._running = True
        self.logger.info("市场数据连接建立完成")

    async def disconnect(self) -> None:
        """
        断开WebSocket和REST连接，停止消息处理

        断开过程：
        1. 设置停止标志
        2. 断开WebSocket连接
        3. 关闭HTTP会话
        4. 清理资源
        5. 记录断开日志
        """
        self.logger.info("开始断开市场数据连接...")
        self._running = False
        await self._ws_manager.disconnect()
        if self._session:
            await self._session.close()
            self._session = None
        self.logger.info("市场数据连接已断开")

    async def _handle_messages(self, data: dict) -> None:
        """
        WebSocket消息主循环，处理深度和成交推送，自动重连

        消息处理流程：
        1. 处理订阅响应消息
        2. 处理订单簿更新消息
        3. 处理成交消息

        Args:
            data: WebSocket消息数据，包含消息类型和具体内容
        """
        if 'event' in data:
            self._handle_subscription_event(data)
        elif 'arg' in data and data['arg']['channel'] == 'books':
            self.logger.debug(f"收到订单簿更新消息: {data['arg']['instId']}")
            await self._handle_orderbook_update(data)
        elif 'arg' in data and data['arg']['channel'] == 'trades':
            self.logger.debug(f"收到成交消息: {data['arg']['instId']}")
            await self._handle_trade(data)

    async def _handle_orderbook_snapshot(self, data: dict) -> None:
        """处理订单簿快照消息

        处理OKX WebSocket的订单簿更新消息，包括：
        1. 维护订单簿状态
        2. 通知订阅者

        Args:
            data: 订单簿快照数据，包含买单和卖单的完整
        """
        # TODO: Implement OKX orderbook snapshot handling
        pass

    async def _handle_orderbook_update(self, data: dict) -> None:
        """
        处理订单簿更新消息

        处理流程：
        1. 转换交易对符号
        2. 检查是否需要获取快照
        3. 验证更新序号
        4. 合并更新到本地状态
        5. 通知订阅者

        Args:
            data: 订单簿更新数据，包含：
                - arg: 订阅参数
                - data: 订单簿数据
                    - ts: 时间戳
                    - checksum: 校验和
                    - bids: 买单更新
                    - asks: 卖单更新
        """
        system_symbol = from_exchange(data['arg']['instId'], self._symbol_adapter())
        self.logger.debug(f"处理{system_symbol}的订单簿更新...")

        # 如果没有快照，先获取快照
        if system_symbol not in self._orderbook_snapshot_cache:
            self.logger.info(f"{system_symbol}没有订单簿快照，开始同步...")
            if not await self._ensure_last_update_id_is_greater_than_U(system_symbol, data):
                self.logger.error(f"为{system_symbol}同步初始订单簿快照失败或操作被中断，放弃处理当前消息")
                return

        # 获取更新序号
        u_in_last_orderbook_update = data['data'][0]['ts']
        last_update_id_in_snapshot = self._orderbook_snapshot_cache[system_symbol].aux_data['lastUpdateId']

        # 验证更新序号
        if self._u_is_less_than_last_update_id(system_symbol, data):
            self.logger.info(f"增量订单簿中最后的更新({system_symbol}, u={u_in_last_orderbook_update})早于快照(lastUpdateId={last_update_id_in_snapshot})，清空{system_symbol}的增量订单簿并跳过当前消息")
            return
        else:
            self.logger.debug(f"增量订单簿中最后的更新({system_symbol}, u={u_in_last_orderbook_update})晚于快照(lastUpdateId={last_update_id_in_snapshot})，开始合并{system_symbol}的增量订单簿")

        # 合并更新并通知
        self._merge_orderbook_update_to_snapshot(system_symbol, data)
        self.logger.debug(f"合并后{system_symbol}买盘档数: {len(self._orderbook_snapshot_cache[system_symbol].bids)}, 卖盘档数: {len(self._orderbook_snapshot_cache[system_symbol].asks)}")
        await self._notify_orderbook(self._orderbook_snapshot_cache[system_symbol])

    async def _handle_trade(self, data: dict) -> None:
        """
        处理成交消息

        处理流程：
        1. 转换交易对符号
        2. 获取成交ID
        3. 创建成交对象
        4. 通知订阅者

        Args:
            data: 成交数据，包含：
                - arg: 订阅参数
                - data: 成交数据列表
                    - instId: 交易对
                    - tradeId: 成交ID
                    - px: 成交价格
                    - sz: 成交数量
                    - side: 成交方向
                    - ts: 成交时间
        """
        system_symbol = from_exchange(data['arg']['instId'], self._symbol_adapter())
        trade_id = data['data'][0]['tradeId']
        self.logger.debug(f"处理{system_symbol}的成交消息，成交ID: {trade_id}")

        # 创建成交对象
        trade = Trade(
            symbol=system_symbol,
            price=Decimal(data['data'][0]['px']),
            quantity=Decimal(data['data'][0]['sz']),
            side='sell' if data['data'][0]['side'] == 'sell' else 'buy',
            timestamp=int(data['data'][0]['ts']),
            trade_id=trade_id
        )
        await self._notify_trade(trade)
        self.logger.debug(f"已处理{system_symbol}的成交消息，价格: {trade.price}, 数量: {trade.quantity}, 方向: {trade.side}")

    def _u_is_less_than_last_update_id(self, symbol: str, data: dict) -> bool:
        """
        判断增量订单簿的u是否早于快照的lastUpdateId

        用于验证增量更新的有效性，确保不会使用过期的更新数据。

        Args:
            symbol: 系统内部symbol
            data: 增量数据，包含ts字段

        Returns:
            bool: True表示应丢弃该增量，False表示可以使用该增量

        Note:
            - 如果快照不存在，返回False
            - 如果数据中缺少ts字段，返回False
            - 如果快照中缺少lastUpdateId，返回False
        """
        orderbook_snapshot = self._orderbook_snapshot_cache.get(symbol)
        if not orderbook_snapshot:
            self.logger.warning(f"没有找到{symbol}的订单簿快照用于比较lastUpdateId和增量订单簿中的'ts'")
            return False
        if 'data' not in data or not data['data'] or 'ts' not in data['data'][0]:
            self.logger.warning(f"收到的{symbol}的增量订单簿数据中没有字段'ts'")
            return False
        if 'lastUpdateId' not in orderbook_snapshot.aux_data:
            self.logger.warning(f"{symbol}的订单簿快照中没有字段lastUpdateId")
            return False
        u_in_last_orderbook_update = data['data'][0]['ts']
        last_update_id_in_snapshot = orderbook_snapshot.aux_data['lastUpdateId']
        return u_in_last_orderbook_update < last_update_id_in_snapshot

    async def _ensure_last_update_id_is_greater_than_U(self, symbol: str, data: dict) -> bool:
        """
        反复获取订单簿快照，直到其lastUpdateId大于增量订单簿的U值

        用于确保快照数据的有效性，防止使用过期的快照数据。

        Args:
            symbol: 系统内部symbol
            data: 增量数据，包含ts字段

        Returns:
            bool: True表示快照同步成功，False表示同步失败

        Note:
            - 会不断重试直到成功或服务停止
            - 每次重试间隔1秒
            - 如果服务停止，会返回False
        """
        self.logger.info(f"正在为 {symbol} 获取同步订单簿快照...")
        while self._running:
            await self._get_orderbook_snapshot(symbol)
            if symbol not in self._orderbook_snapshot_cache or \
               not self._orderbook_snapshot_cache.get(symbol) or \
               'lastUpdateId' not in self._orderbook_snapshot_cache[symbol].aux_data:
                self.logger.warning(f"获取 {symbol} 的订单簿快照失败或快照无效，1秒后重试...")
                await asyncio.sleep(1)
                continue
            last_update_id_in_snapshot = self._orderbook_snapshot_cache[symbol].aux_data['lastUpdateId']
            u_in_last_orderbook_update = data['data'][0]['ts']
            if last_update_id_in_snapshot > u_in_last_orderbook_update:
                self.logger.info(f"快照同步条件满足 for {symbol}: snapshot_lastUpdateId ({last_update_id_in_snapshot}) > data['ts'] ({data['data'][0]['ts']})")
                self.logger.info(f"成功获取并同步了 {symbol} 的订单簿快照。")
                return True
            else:
                self.logger.info(f"快照 for {symbol} (lastUpdateId: {last_update_id_in_snapshot}) "
                      f"未满足条件 (未大于增量订单簿的 'ts' 值: {u_in_last_orderbook_update})。1秒后重试获取快照...")
            await asyncio.sleep(1)
        self.logger.info(f"为 {symbol} 同步订单簿快照的操作因服务停止而被中断。")
        return False

    async def _get_orderbook_snapshot(self, symbol: str) -> None:
        """
        获取订单簿快照

        通过REST API获取指定交易对的订单簿快照。

        Args:
            symbol: 系统内部symbol

        处理流程：
        1. 检查HTTP会话是否存在
        2. 发送REST API请求
        3. 解析响应数据
        4. 创建订单簿对象
        5. 更新本地缓存
        6. 通知订阅者

        Note:
            - 如果HTTP会话不存在，会记录警告并返回
            - 如果请求失败，会记录错误
            - 如果发生异常，会记录错误
        """
        self.logger.info(f"开始获取{symbol}的订单簿快照...")
        if not self._session:
            self.logger.warning(f"为{symbol}获取订单簿快照的session为空")
            return
        try:
            url = f"{self._rest_url}/api/v5/market/books"
            params = {
                "instId": to_exchange(symbol, self._symbol_adapter()),
                "sz": self._orderbook_depth_limit
            }
            async with self._session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data['code'] == '0':
                        orderbook_data = data['data'][0]
                        orderbook = OrderBook(
                            symbol=symbol,
                            bids=SortedDict(lambda x: -x),
                            asks=SortedDict(),
                            timestamp=int(orderbook_data['ts']),
                            aux_data={'lastUpdateId': int(orderbook_data['ts'])}
                        )
                        for bid in orderbook_data["bids"]:
                            price = Decimal(bid[0])
                            quantity = Decimal(bid[1])
                            orderbook.bids[price] = OrderBookLevel(price, quantity)
                        for ask in orderbook_data["asks"]:
                            price = Decimal(ask[0])
                            quantity = Decimal(ask[1])
                            orderbook.asks[price] = OrderBookLevel(price, quantity)
                        self._orderbook_snapshot_cache[symbol] = orderbook
                        self.logger.info(f"成功获取{symbol}的订单簿快照，买盘档数: {len(orderbook.bids)}, 卖盘档数: {len(orderbook.asks)}")
                        await self._notify_orderbook(orderbook)
                    else:
                        self.logger.error(f"获取{symbol}订单簿快照失败，错误码: {data['code']}, 错误信息: {data['msg']}")
                else:
                    self.logger.error(f"获取{symbol}订单簿快照失败，HTTP状态码: {response.status}")
        except Exception as e:
            self.logger.error(f"获取{symbol}订单簿快照时发生错误: {e}")

    def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBook], Union[None, Awaitable[None]]]) -> None:
        """
        订阅订单簿数据

        订阅指定交易对的订单簿数据，包括：
        1. 注册回调函数
        2. 发送WebSocket订阅消息

        Args:
            symbol: 交易对符号，例如 'BTCUSDT'
            callback: 订单簿数据回调函数，可以是同步或异步函数

        Note:
            - 如果WebSocket未连接，只会注册回调函数
            - 订阅消息会包含更新间隔设置
        """
        self.logger.info(f"开始订阅{symbol}的订单簿数据...")
        super().subscribe_orderbook(symbol, callback)
        if self._ws_manager._ws:
            exchange_symbol = to_exchange(symbol, self._symbol_adapter())
            subscribe_msg = {
                "op": "subscribe",
                "args": [{
                    "channel": "books",
                    "instId": exchange_symbol
                }]
            }
            self._subscription_requests[self._next_request_id] = subscribe_msg
            self._next_request_id += 1
            self.logger.info(f"发送订单簿订阅请求: {subscribe_msg}")
            asyncio.create_task(self._ws_manager.send_message(subscribe_msg))

    def subscribe_trades(self, symbol: str, callback: Callable[[Trade], Union[None, Awaitable[None]]]) -> None:
        """
        订阅逐笔成交数据

        订阅指定交易对的成交数据，包括：
        1. 注册回调函数
        2. 发送WebSocket订阅消息

        Args:
            symbol: 交易对符号，例如 'BTCUSDT'
            callback: 成交数据回调函数，可以是同步或异步函数

        Note:
            - 如果WebSocket未连接，只会注册回调函数
            - 不同市场类型使用不同的事件类型
        """
        self.logger.info(f"开始订阅{symbol}的成交数据...")
        super().subscribe_trades(symbol, callback)
        if self._ws_manager._ws:
            exchange_symbol = to_exchange(symbol, self._symbol_adapter())
            subscribe_msg = {
                "op": "subscribe",
                "args": [{
                    "channel": "trades",
                    "instId": exchange_symbol
                }]
            }
            self._subscription_requests[self._next_request_id] = subscribe_msg
            self._next_request_id += 1
            self.logger.info(f"发送成交订阅请求: {subscribe_msg}")
            asyncio.create_task(self._ws_manager.send_message(subscribe_msg))

    def resubscribe_all(self):
        """重新订阅所有已订阅的交易对数据"""
        # TODO: Implement resubscription logic
        pass

    def _merge_orderbook_update_to_snapshot(self, symbol: str, data: dict) -> None:
        """
        将增量订单簿数据合并到本地快照

        处理流程：
        1. 获取本地快照
        2. 更新买单
        3. 更新卖单
        4. 更新时间戳

        Args:
            symbol: 系统内部symbol
            data: 增量数据，包含：
                - data: 订单簿数据列表
                    - ts: 时间戳
                    - bids: 买单更新列表
                    - asks: 卖单更新列表

        Note:
            - 数量为0的档位会被删除
            - 新的档位会被添加
            - 已存在的档位会被更新
        """
        orderbook = self._orderbook_snapshot_cache[symbol]
        for bid in data['data'][0]['bids']:
            price = Decimal(bid[0])
            quantity = Decimal(bid[1])
            if quantity == 0:
                orderbook.bids.pop(price, None)
            else:
                orderbook.bids[price] = OrderBookLevel(price, quantity)
        for ask in data['data'][0]['asks']:
            price = Decimal(ask[0])
            quantity = Decimal(ask[1])
            if quantity == 0:
                orderbook.asks.pop(price, None)
            else:
                orderbook.asks[price] = OrderBookLevel(price, quantity)
        orderbook.timestamp = int(data['data'][0]['ts'])

    def _handle_subscription_event(self, data: dict) -> None:
        """
        处理订阅事件响应

        处理WebSocket订阅请求的响应消息，包括：
        1. 订阅成功
        2. 订阅失败
        3. 订阅错误

        Args:
            data: 订阅响应数据，包含：
                - event: 事件类型
                - code: 响应码
                - msg: 响应消息

        Note:
            - 成功响应中code为0
            - 失败响应中code不为0
            - 错误响应中包含msg字段
        """
        if data['event'] == 'subscribe':
            if data['code'] == '0':
                self.logger.info(f"订阅成功: {data}")
            else:
                self.logger.error(f"订阅失败: {data}")
        elif data['event'] == 'error':
            self.logger.error(f"订阅错误: {data}")
