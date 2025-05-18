import json
import asyncio
from typing import Dict, List, Optional, Callable, Union, Awaitable
from decimal import Decimal
import websockets
import aiohttp
from .base import MarketDataBase, OrderBook, OrderBookLevel, Trade
from .config import BinancePerpConfig
from sortedcontainers import SortedDict
from util.logger import get_logger
from util.symbol_convert import to_exchange, from_exchange

class BinancePerpMarketData(MarketDataBase):
    """
    币安永续合约市场数据实现（USDT本位和币本位）
    支持：
    - 订单簿（orderbook）订阅与快照
    - 逐笔成交（trade）订阅
    - 自动重连、快照同步、合约类型切换
    """
    def __init__(self, config: BinancePerpConfig = BinancePerpConfig(), contract_type: str = 'usdt'):
        """
        初始化市场数据对象
        :param config: 配置对象，包含WS/REST地址等
        :param contract_type: 'usdt' 表示USDT本位，'coin' 表示币本位
        """
        super().__init__()
        self.logger = get_logger("BinancePerpMarketData")
        self._config = config
        assert contract_type in ('usdt', 'coin')  # 只允许两种类型
        self._contract_type = contract_type
        self._ws_url = config.WS_URLS[contract_type]
        self._rest_url = config.REST_URLS[contract_type]
        self._orderbook_depth_limit = config.ORDERBOOK_DEPTH_LIMIT
        self._orderbook_update_interval = config.ORDERBOOK_UPDATE_INTERVAL
        self._ws: Optional[websockets.WebSocketClientProtocol] = None  # WebSocket连接对象
        self._session: Optional[aiohttp.ClientSession] = None  # HTTP会话对象
        self._running = False  # 控制消息循环
        self._message_handler_task: Optional[asyncio.Task] = None  # 消息处理任务
        self._orderbook_snapshot_cache: Dict[str, OrderBook] = {}  # symbol -> 订单簿快照
        self._next_request_id = 1  # WebSocket请求ID

    def _symbol_adapter(self):
        """
        获取当前合约类型对应的symbol适配器名
        :return: 'binance_perp_usdt' 或 'binance_perp_coin'
        """
        return f"binance_perp_{self._contract_type}"

    async def connect(self) -> None:
        """
        建立WebSocket和REST连接，并启动消息处理循环
        """
        self._ws = await websockets.connect(self._ws_url)
        self._session = aiohttp.ClientSession()
        self._running = True
        self._message_handler_task = asyncio.create_task(self._handle_messages())
        self.resubscribe_all()

    async def disconnect(self) -> None:
        """
        断开WebSocket和REST连接，停止消息处理
        """
        self._running = False
        if self._message_handler_task:
            self._message_handler_task.cancel()
            try:
                await self._message_handler_task
            except asyncio.CancelledError:
                pass
            self._message_handler_task = None
        if self._ws:
            try:
                async with asyncio.timeout(3):
                    await self._ws.close()
            except (asyncio.TimeoutError, Exception):
                self._ws.fail_connection()
            finally:
                self._ws = None
        if self._session:
            await self._session.close()
            self._session = None

    async def _handle_messages(self) -> None:
        """
        WebSocket消息主循环，处理深度和成交推送，自动重连
        """
        while self._running:
            try:
                if not self._ws:
                    await asyncio.sleep(1)
                    continue

                message = await self._ws.recv()
                data = json.loads(message)
                # 订单簿增量更新
                if 'e' in data and data['e'] == 'depthUpdate':
                    await self._handle_orderbook_update(data)
                # 逐笔成交
                elif 'e' in data and data['e'] == 'aggTrade':
                    await self._handle_trade(data)
            except websockets.exceptions.ConnectionClosed:
                # 连接断开自动重连
                if self._running:
                    self.logger.warning("连接已断开，正在重连...")
                    await asyncio.sleep(1)
                    try:
                        self._ws = await websockets.connect(self._ws_url)
                        self.resubscribe_all()
                    except Exception as e:
                        self.logger.error(f"重连失败: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                # 其他异常也尝试重连
                if self._running:
                    self.logger.error(f"处理消息时出错: {e}")
                    await asyncio.sleep(1)
                    try:
                        self._ws = await websockets.connect(self._ws_url)
                        self.resubscribe_all()
                    except Exception as e:
                        self.logger.error(f"重连失败: {e}")

    async def _handle_orderbook_update(self, data: dict) -> None:
        """
        处理订单簿增量更新消息，自动同步快照并合并增量
        :param data: WebSocket收到的订单簿增量数据
        """
        system_symbol = from_exchange(data['s'], self._symbol_adapter())
        # 若无快照，先同步快照
        if system_symbol not in self._orderbook_snapshot_cache:
            if not await self._ensure_last_update_id_is_greater_than_U(system_symbol, data):
                self.logger.error(f"为 {system_symbol} 同步初始订单簿快照失败或操作被中断，放弃处理当前消息。")
                return
        u_in_last_orderbook_update = data['u']
        last_update_id_in_snapshot = self._orderbook_snapshot_cache[system_symbol].aux_data['lastUpdateId']
        # 增量早于快照则丢弃
        if self._u_is_less_than_last_update_id(system_symbol, data):
            self.logger.info(f"增量订单簿中最后的更新 ({system_symbol}, u={u_in_last_orderbook_update}) 早于快照 (lastUpdateId={last_update_id_in_snapshot})。清空 {system_symbol} 的增量订单簿并跳过当前消息。")
            return
        else:
            self.logger.debug(f"增量订单簿中最后的更新 ({system_symbol}, u={u_in_last_orderbook_update}) 晚于快照 (lastUpdateId={last_update_id_in_snapshot})。合并 {system_symbol} 的增量订单簿。")
        # 合并增量到快照
        self._merge_orderbook_update_to_snapshot(system_symbol, data)
        self.logger.debug(f"合并后 {system_symbol} 买盘档数: {len(self._orderbook_snapshot_cache[system_symbol].bids)}, 卖盘档数: {len(self._orderbook_snapshot_cache[system_symbol].asks)}")
        await self._notify_orderbook(self._orderbook_snapshot_cache[system_symbol])

    async def _handle_trade(self, data: dict) -> None:
        """
        处理逐笔成交消息，转换为Trade对象并通知订阅者
        :param data: WebSocket收到的成交数据
        """
        system_symbol = from_exchange(data['s'], self._symbol_adapter())
        trade = Trade(
            symbol=system_symbol,
            price=Decimal(data['p']),
            quantity=Decimal(data['q']),
            side='sell' if data['m'] else 'buy',
            timestamp=data['E'],
            trade_id=str(data['a'])
        )
        await self._notify_trade(trade)

    def _u_is_less_than_last_update_id(self, symbol: str, data: dict) -> bool:
        """
        判断增量订单簿的u是否早于快照的lastUpdateId
        :param symbol: 系统内部symbol
        :param data: 增量数据
        :return: True表示应丢弃该增量
        """
        orderbook_snapshot = self._orderbook_snapshot_cache.get(symbol)
        if not orderbook_snapshot:
            self.logger.warning(f"没有找到{symbol}的订单簿快照用于比较lastUpdateId和增量订单簿中的'u'")
            return False
        if 'u' not in data:
            self.logger.warning(f"收到的{symbol}的增量订单簿数据中没有字段'u'")
            return False
        if 'lastUpdateId' not in orderbook_snapshot.aux_data:
            self.logger.warning(f"{symbol}的订单簿快照中没有字段lastUpdateId")
            return False
        u_in_last_orderbook_update = data['u']
        last_update_id_in_snapshot = orderbook_snapshot.aux_data['lastUpdateId']
        return u_in_last_orderbook_update < last_update_id_in_snapshot

    async def _ensure_last_update_id_is_greater_than_U(self, symbol: str, data: dict) -> bool:
        """
        反复获取订单簿快照，直到其lastUpdateId大于增量订单簿的U值
        :param symbol: 系统内部symbol
        :param data: 增量数据
        :return: True表示快照同步成功
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
            u_in_last_orderbook_update = data['U']
            if last_update_id_in_snapshot > u_in_last_orderbook_update:
                self.logger.info(f"快照同步条件满足 for {symbol}: snapshot_lastUpdateId ({last_update_id_in_snapshot}) > data['U'] ({data['U']})")
                self.logger.info(f"成功获取并同步了 {symbol} 的订单簿快照。")
                return True
            await asyncio.sleep(1)
        self.logger.info(f"为 {symbol} 同步订单簿快照的操作因服务停止而被中断。")
        return False

    async def _get_orderbook_snapshot(self, symbol: str) -> None:
        """
        通过REST API获取完整订单簿快照，并初始化本地订单簿
        :param symbol: 系统内部symbol
        """
        if not self._session:
            return
        try:
            url = f"{self._rest_url}/depth"
            params = {
                "symbol": to_exchange(symbol, self._symbol_adapter()),
                "limit": self._orderbook_depth_limit
            }
            async with self._session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    orderbook = OrderBook(
                        symbol=symbol,
                        bids=SortedDict(lambda x: -x),  # 买单降序
                        asks=SortedDict(),              # 卖单升序
                        timestamp=0,
                        aux_data={'lastUpdateId': data['lastUpdateId']}
                    )
                    for bid in data["bids"]:
                        price = Decimal(bid[0])
                        quantity = Decimal(bid[1])
                        orderbook.bids[price] = OrderBookLevel(price, quantity)
                    for ask in data["asks"]:
                        price = Decimal(ask[0])
                        quantity = Decimal(ask[1])
                        orderbook.asks[price] = OrderBookLevel(price, quantity)
                    self._orderbook_snapshot_cache[symbol] = orderbook
                    self.logger.debug(f"快照 {symbol} 买盘档数: {len(orderbook.bids)}, 卖盘档数: {len(orderbook.asks)}")
                    await self._notify_orderbook(orderbook)
                else:
                    self.logger.error(f"获取订单簿快照失败: {response.status}")
        except Exception as e:
            self.logger.error(f"获取订单簿快照时出错: {e}")

    def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBook], Union[None, Awaitable[None]]]) -> None:
        """
        订阅指定symbol的订单簿数据，自动发送WebSocket订阅消息
        :param symbol: 例如 'BTC-USDT-PERP'
        :param callback: 订单簿回调
        """
        super().subscribe_orderbook(symbol, callback)
        if self._ws:
            exchange_symbol = to_exchange(symbol, self._symbol_adapter())
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [f"{exchange_symbol.lower()}@depth@{self._orderbook_update_interval}"],
                "id": self._next_request_id
            }
            self._next_request_id += 1
            asyncio.create_task(self._ws.send(json.dumps(subscribe_msg)))

    def subscribe_trades(self, symbol: str, callback: Callable[[Trade], Union[None, Awaitable[None]]]) -> None:
        """
        订阅指定symbol的逐笔成交数据，自动发送WebSocket订阅消息
        :param symbol: 例如 'BTC-USDT-PERP'
        :param callback: 成交回调
        """
        super().subscribe_trades(symbol, callback)
        if self._ws:
            exchange_symbol = to_exchange(symbol, self._symbol_adapter())
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [f"{exchange_symbol.lower()}@aggTrade"],
                "id": self._next_request_id
            }
            self._next_request_id += 1
            asyncio.create_task(self._ws.send(json.dumps(subscribe_msg)))

    def _merge_orderbook_update_to_snapshot(self, symbol: str, data: dict) -> None:
        """
        将增量订单簿数据合并到本地快照
        :param symbol: 系统内部symbol
        :param data: 增量数据
        """
        orderbook = self._orderbook_snapshot_cache[symbol]
        for bid in data['b']:
            price = Decimal(bid[0])
            quantity = Decimal(bid[1])
            if quantity == 0:
                orderbook.bids.pop(price, None)
            else:
                orderbook.bids[price] = OrderBookLevel(price, quantity)
        for ask in data['a']:
            price = Decimal(ask[0])
            quantity = Decimal(ask[1])
            if quantity == 0:
                orderbook.asks.pop(price, None)
            else:
                orderbook.asks[price] = OrderBookLevel(price, quantity)
        orderbook.timestamp = data['E']

    def resubscribe_all(self):
        """
        重新订阅所有已注册的symbol，重建快照缓存
        """
        self._orderbook_snapshot_cache.clear()
        super().resubscribe_all()
