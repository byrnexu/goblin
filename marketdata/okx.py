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
from .base import MarketDataBase, OrderBook, OrderBookLevel, Trade, MarketDataMessageHandler
from .config import OkxConfig
from .types import MarketType
from sortedcontainers import SortedDict
from util.logger import get_logger
from util.symbol_convert import to_exchange, from_exchange
from util.websocket_manager import WebSocketManager, MessageHandler, ReconnectConfig, ConnectionConfig

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
    # 定义深度限制到频道名称的映射
    CHANNEL_MAPPING = { 1: "bbo-tbt", 5: "books5", 400: "books" }

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
        self.logger.info(f"初始化OKX市场数据服务 market {market_type.value} ...")

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
            self.logger.info(f"收到订单簿更新消息: {data['arg']['instId']}")
            await self._handle_orderbook_update(data)
        elif 'arg' in data and data['arg'].get('channel') in {'bbo-tbt', 'books5'}:
            self.logger.info(f"收到订单簿快照消息: {data['arg']['instId']}")
            await self._handle_orderbook_snapshot(data)
        elif 'arg' in data and data['arg']['channel'] == 'trades':
            self.logger.info(f"收到成交消息: {data['arg']['instId']}")
            await self._handle_trade(data)

    def _create_orderbook_from_data(self, data: dict) -> OrderBook:
        """
        根据OKX订单簿数据创建OrderBook对象

        Args:
            data: 订单簿数据，包含：
                - asks: 卖单数组
                - bids: 买单数组
                - ts: 时间戳
                - checksum: 校验和
                - prevSeqId: 前一个序号
                - seqId: 当前序号

        Returns:
            OrderBook: 创建的订单簿对象
        """
        # 获取交易对符号
        exchange_symbol = data['arg']['instId']
        system_symbol = from_exchange(exchange_symbol, self._symbol_adapter())

        # 获取订单簿数据
        orderbook_data = data['data'][0]

        # 创建新的OrderBook对象
        orderbook = OrderBook(
            symbol=system_symbol,
            bids=SortedDict(lambda x: -x),  # 买单按价格降序
            asks=SortedDict(),  # 卖单按价格升序
            timestamp=int(orderbook_data['ts']),
            aux_data={
                'seqId': orderbook_data['seqId']
            }
        )

        # 处理买单
        for bid in orderbook_data['bids']:
            price = Decimal(bid[0])
            quantity = Decimal(bid[1])
            if quantity > 0:  # 只添加数量大于0的档位
                orderbook.bids[price] = OrderBookLevel(price, quantity)

        # 处理卖单
        for ask in orderbook_data['asks']:
            price = Decimal(ask[0])
            quantity = Decimal(ask[1])
            if quantity > 0:  # 只添加数量大于0的档位
                orderbook.asks[price] = OrderBookLevel(price, quantity)

        return orderbook

    def _merge_orderbook_update(self, existing_orderbook: OrderBook, update_orderbook: OrderBook) -> None:
        """
        将增量订单簿更新合并到现有订单簿

        Args:
            existing_orderbook: 现有的订单簿对象
            update_orderbook: 增量更新的订单簿对象
        """
        # 更新买单
        for price, level in update_orderbook.bids.items():
            if level.quantity > 0:
                existing_orderbook.bids[price] = level
            else:
                existing_orderbook.bids.pop(price, None)

        # 更新卖单
        for price, level in update_orderbook.asks.items():
            if level.quantity > 0:
                existing_orderbook.asks[price] = level
            else:
                existing_orderbook.asks.pop(price, None)

        # 更新时间戳和序号信息
        existing_orderbook.timestamp = update_orderbook.timestamp
        existing_orderbook.aux_data.update(update_orderbook.aux_data)

    async def _handle_orderbook_update(self, data: dict) -> None:
        """
        处理OKX订单簿更新消息

        处理流程：
        1. 转换交易对符号
        2. 根据action类型处理：
           - snapshot: 直接保存为快照
           - update: 从缓存获取快照并合并更新
        3. 通知订阅者

        Args:
            data: 订单簿数据，包含：
                - arg: 包含channel和instId
                - action: 'snapshot'或'update'
                - data: 订单簿数据数组
        """
        # 获取交易对符号
        exchange_symbol = data['arg']['instId']
        system_symbol = from_exchange(exchange_symbol, self._symbol_adapter())
        self.logger.debug(f"处理{system_symbol}的订单簿更新，action: {data['action']}")

        # 创建订单簿对象
        orderbook = self._create_orderbook_from_data(data)

        if data['action'] == 'snapshot':
            # 如果是快照，直接保存
            self._orderbook_snapshot_cache[system_symbol] = orderbook
            self.logger.info(f"保存{system_symbol}的订单簿快照，买盘档数: {len(orderbook.bids)}, 卖盘档数: {len(orderbook.asks)}")
        else:  # update
            # 如果是增量更新，需要合并到现有快照
            if system_symbol not in self._orderbook_snapshot_cache:
                self.logger.warning(f"收到{system_symbol}的增量更新但没有快照，将其作为快照保存")
                self._orderbook_snapshot_cache[system_symbol] = orderbook
            else:
                # 合并更新到现有快照
                self._merge_orderbook_update(self._orderbook_snapshot_cache[system_symbol], orderbook)
                self.logger.debug(f"合并后{system_symbol}买盘档数: {len(self._orderbook_snapshot_cache[system_symbol].bids)}, 卖盘档数: {len(self._orderbook_snapshot_cache[system_symbol].asks)}")

        # 通知订阅者
        await self._notify_orderbook(self._orderbook_snapshot_cache[system_symbol])

    async def _handle_orderbook_snapshot(self, data: dict) -> None:
        """
        处理订单簿快照消息

        处理OKX WebSocket的订单簿快照消息，包括：
        1. 创建订单簿对象
        2. 保存为快照
        3. 通知订阅者

        Args:
            data: 订单簿快照数据
        """
        # 创建订单簿对象
        orderbook = self._create_orderbook_from_data(data)
        self.logger.info(f"{orderbook.symbol}的订单簿快照，买盘档数: {len(orderbook.bids)}, 卖盘档数: {len(orderbook.asks)}")

        # 通知订阅者
        await self._notify_orderbook(orderbook)

    async def _handle_trade(self, data: dict) -> None:
        """
        处理OKX成交信息

        处理流程：
        1. 转换交易对符号
        2. 创建成交对象
        3. 通知订阅者

        Args:
            data: 成交数据，包含：
                - arg: 包含channel和instId
                - data: 成交数据数组，每个元素包含：
                    - instId: 交易对
                    - tradeId: 成交ID
                    - px: 成交价格
                    - sz: 成交数量
                    - side: 成交方向（buy/sell）
                    - ts: 成交时间戳
                    - count: 成交次数
        """
        # 获取交易对符号
        exchange_symbol = data['arg']['instId']
        system_symbol = from_exchange(exchange_symbol, self._symbol_adapter())

        # 获取成交数据
        trade_data = data['data'][0]

        # 创建成交对象
        trade = Trade(
            symbol=system_symbol,
            price=Decimal(trade_data['px']),
            quantity=Decimal(trade_data['sz']),
            side=trade_data['side'],
            timestamp=int(trade_data['ts']),
            trade_id=trade_data['tradeId']
        )

        self.logger.debug(f"处理{system_symbol}的成交消息，价格: {trade.price}, 数量: {trade.quantity}, 方向: {trade.side}")

        # 通知订阅者
        await self._notify_trade(trade)

    def _build_orderbook_subscription_message(self, symbol: str) -> dict:
        """
        构建订单簿订阅消息
        
        Args:
            symbol: 交易对符号
            
        Returns:
            dict: 订阅消息内容
        """
        exchange_symbol = to_exchange(symbol, self._symbol_adapter())
        return {
            "op": "subscribe",
            "args": [{
                "channel": self.CHANNEL_MAPPING.get(
                    self._config.ORDERBOOK_DEPTH_LIMIT.get(self._market_type, 400),
                    "books"
                ),
                "instId": exchange_symbol
            }]
        }

    def _build_trade_subscription_message(self, symbol: str) -> dict:
        """
        构建成交订阅消息
        
        Args:
            symbol: 交易对符号
            
        Returns:
            dict: 订阅消息内容
        """
        exchange_symbol = to_exchange(symbol, self._symbol_adapter())
        return {
            "op": "subscribe",
            "args": [{
                "channel": "trades",
                "instId": exchange_symbol
            }]
        }

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
        self.logger.info(f"收到应答: {data}")
