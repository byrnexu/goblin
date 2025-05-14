import json
import asyncio
from typing import Dict, List, Optional, Callable, Union, Awaitable
from decimal import Decimal
import websockets
import aiohttp
from .base import MarketDataBase, OrderBook, OrderBookLevel, Trade
from .config import BinanceConfig

class BinanceMarketData(MarketDataBase):
    """币安交易所市场数据实现

    实现了币安交易所的WebSocket API，提供实时市场数据订阅功能。
    包括：
    1. 订单簿数据订阅和更新
    2. 逐笔成交数据订阅
    3. 自动重连机制
    4. 订单簿快照获取
    """

    def __init__(self, config: BinanceConfig = BinanceConfig()):
        super().__init__()

        self._config = config
        self._ws_url = config.WS_URL
        self._rest_url = config.REST_URL
        self._orderbook_depth_limit = config.ORDERBOOK_DEPTH_LIMIT
        self._orderbook_update_interval = config.ORDERBOOK_UPDATE_INTERVAL

        # WebSocket连接对象
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        # HTTP会话，用于REST API请求
        self._session: Optional[aiohttp.ClientSession] = None

        # 控制消息处理循环的运行状态
        self._running = False
        # 消息处理任务
        self._message_handler_task: Optional[asyncio.Task] = None

        # 存储每个交易对的订单簿数据
        self._orderbook_snapshot_cache: Dict[str, OrderBook] = {}

    async def connect(self) -> None:
        """连接到币安WebSocket服务器

        建立WebSocket连接并启动消息处理循环
        同时创建HTTP会话用于REST API请求
        """
        self._ws = await websockets.connect(self._ws_url)
        self._session = aiohttp.ClientSession()

        self._running = True
        self._message_handler_task = asyncio.create_task(self._handle_messages())

    async def disconnect(self) -> None:
        """断开与币安WebSocket服务器的连接

        1. 停止消息处理循环
        2. 取消消息处理任务
        3. 关闭WebSocket连接
        4. 关闭HTTP会话
        """
        self._running = False

        # 取消消息处理任务
        if self._message_handler_task:
            self._message_handler_task.cancel()
            try:
                await self._message_handler_task
            except asyncio.CancelledError:
                pass
            self._message_handler_task = None

        # 关闭WebSocket连接
        if self._ws:
            try:
                # 设置3秒超时
                async with asyncio.timeout(3):
                    await self._ws.close()
            except (asyncio.TimeoutError, Exception):
                # 如果超时或发生其他错误，强制关闭连接
                self._ws.fail_connection()
            finally:
                self._ws = None

        # 关闭HTTP会话
        if self._session:
            await self._session.close()
            self._session = None

    async def _handle_messages(self) -> None:
        """处理WebSocket消息

        持续接收并处理WebSocket消息，包括：
        1. 订单簿更新消息
        2. 成交消息
        3. 错误处理
        4. 自动重连
        """
        if not self._ws:
            return

        while self._running:
            try:
                message = await self._ws.recv()
                data = json.loads(message)

                # 处理订单簿数据
                if 'e' in data and data['e'] == 'depthUpdate':
                    await self._handle_orderbook_update(data)
                # 处理成交数据
                elif 'e' in data and data['e'] == 'trade':
                    await self._handle_trade(data)
            except websockets.exceptions.ConnectionClosed as e:
                if self._running:
                    print(f"连接已断开，正在重连...")
                    await asyncio.sleep(1)
                    await self.connect()
            except asyncio.CancelledError:
                # 任务被取消，正常退出
                break
            except Exception as e:
                if self._running:
                    print(f"处理消息时出错: {e}")
                    await asyncio.sleep(1)
                    await self.connect()

    async def _handle_orderbook_update(self, data: dict) -> None:
        """处理订单簿更新消息

        处理币安WebSocket的订单簿更新消息，包括：
        1. 更新买单
        2. 更新卖单
        3. 维护订单簿状态
        4. 通知订阅者

        Args:
            data: 订单簿更新数据，包含买单和卖单的更新信息
        """
        symbol = data['s']

        # 获取或创建订单簿
        if symbol not in self._orderbook_snapshot_cache:
            # 反复获取订单簿快照，直到其lastUpdateId大于缓存中增量订单簿的U值
            if not await self._ensure_last_update_id_is_greater_than_U(symbol, data):
                print(f"为 {symbol} 同步初始订单簿快照失败或操作被中断，放弃处理当前消息。")
                return # 如果同步失败或被中断，则不处理当前消息

        # 如果增量订单簿中最新的记录的u值小于快照中的lastUpdateId，说明增量订单簿太旧，需要继续获取更新的
        u_in_last_orderbook_update = data['u']
        last_update_id_in_snapshot = self._orderbook_snapshot_cache[symbol].aux_data['lastUpdateId']
        if self._u_is_less_than_last_update_id(symbol, data):
            print(f"增量订单簿中最后的更新 ({symbol}, u={u_in_last_orderbook_update}) 早于快照 (lastUpdateId={last_update_id_in_snapshot})。清空 {symbol} 的增量订单簿并跳过当前消息。")
            return
        else:
            print(f"增量订单簿中最后的更新 ({symbol}, u={u_in_last_orderbook_update}) 晚于快照 (lastUpdateId={last_update_id_in_snapshot})。合并 {symbol} 的增量订单簿。")

        # 将增量更新合并到快照中
        self._merge_orderbook_update_to_snapshot(symbol, data)

        # 通知订单簿已更新
        await self._notify_orderbook(self._orderbook_snapshot_cache[symbol])

    async def _handle_trade(self, data: dict) -> None:
        """处理成交消息

        处理币安WebSocket的成交消息，转换为内部Trade对象并通知订阅者

        Args:
            data: 成交数据，包含价格、数量、方向等信息
        """
        trade = Trade(
            symbol=data['s'],
            price=Decimal(data['p']),
            quantity=Decimal(data['q']),
            side='buy' if data['m'] else 'sell',
            timestamp=data['E'],
            trade_id=str(data['t'])
        )
        await self._notify_trade(trade)

    def _u_is_less_than_last_update_id(self, symbol: str, data: dict) -> bool:
        """Checks if the 'u' of data is less than the snapshot's 'lastUpdateId'."""

        orderbook_snapshot = self._orderbook_snapshot_cache.get(symbol)
        if not orderbook_snapshot:
            print(f"警告: 没有找到{symbol}的订单簿快照用于比较lastUpdateId和增量订单簿中的'u'") # Optional log
            return False

        # Ensure all required keys/data points exist before comparison
        if 'u' not in data:
            print(f"警告: 收到的{symbol}的增量订单簿数据中没有字段'u'")
            return False

        if 'lastUpdateId' not in orderbook_snapshot.aux_data:
            print(f"警告: {symbol}的订单簿快照中没有字段lastUpdateId")
            return False

        u_in_last_orderbook_update = data['u']
        last_update_id_in_snapshot = orderbook_snapshot.aux_data['lastUpdateId']

        return u_in_last_orderbook_update < last_update_id_in_snapshot

    async def _ensure_last_update_id_is_greater_than_U(self, symbol: str, data: dict) -> bool:
        """反复获取订单簿快照，直到其lastUpdateId大于缓存中增量订单簿的U值"""
        print(f"正在为 {symbol} 同步订单簿快照与缓存消息...")
        while self._running:  # 确保在断开连接时停止
            await self._get_orderbook_snapshot(symbol)

            if symbol not in self._orderbook_snapshot_cache or \
               not self._orderbook_snapshot_cache.get(symbol) or \
               'lastUpdateId' not in self._orderbook_snapshot_cache[symbol].aux_data:
                print(f"获取 {symbol} 的订单簿快照失败或快照无效，1秒后重试...")
                await asyncio.sleep(1)
                continue

            last_update_id_in_snapshot = self._orderbook_snapshot_cache[symbol].aux_data['lastUpdateId']
            u_in_last_orderbook_update  = data['U']

            # 条件：快照的lastUpdateId > 缓存消息的U (First update ID in event)
            if last_update_id_in_snapshot > u_in_last_orderbook_update:
                print(f"快照同步条件满足 for {symbol}: snapshot_lastUpdateId ({last_update_id_in_snapshot}) > data['U'] ({data['U']})")
                print(f"成功获取并同步了 {symbol} 的订单簿快照。")
                return True  # 同步成功
            else:
                u_values = [msg.get('U') for msg in orderbook_update_for_symbol if 'U' in msg]
                print(f"快照 for {symbol} (lastUpdateId: {last_update_id_in_snapshot}) "
                      f"未满足条件 (未大于增量订单簿的 'U' 值: {u_in_last_orderbook_update})。1秒后重试获取快照...")
                await asyncio.sleep(1)

        print(f"为 {symbol} 同步订单簿快照的操作因服务停止而被中断。")
        return False # 仅当 self._running 为 False 时到达此处

    async def _get_orderbook_snapshot(self, symbol: str) -> None:
        """获取订单簿快照

        通过REST API获取完整的订单簿数据，用于初始化订单簿状态

        Args:
            symbol: 交易对符号
        """
        if not self._session:
            return

        try:
            # 获取订单簿快照
            url = f"{self._rest_url}/depth"
            params = {
                "symbol": symbol,
                "limit": self._orderbook_depth_limit
            }

            async with self._session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()

                    # 创建订单簿对象
                    orderbook = OrderBook(
                        symbol=symbol,
                        bids=[
                            OrderBookLevel(
                                price=Decimal(bid[0]),
                                quantity=Decimal(bid[1])
                            )
                            for bid in data["bids"]
                        ],
                        asks=[
                            OrderBookLevel(
                                price=Decimal(ask[0]),
                                quantity=Decimal(ask[1])
                            )
                            for ask in data["asks"]
                        ],
                        timestamp=0,
                        aux_data={'lastUpdateId': data['lastUpdateId']} # 将lastUpdateId存入aux_data
                    )

                    # 保存订单簿
                    self._orderbook_snapshot_cache[symbol] = orderbook

                    # 通知更新
                    await self._notify_orderbook(orderbook)
                else:
                    print(f"获取订单簿快照失败: {response.status}")
        except Exception as e:
            print(f"获取订单簿快照时出错: {e}")

    def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBook], Union[None, Awaitable[None]]]) -> None:
        """订阅订单簿数据

        订阅指定交易对的订单簿数据，包括：
        1. 注册回调函数
        2. 发送WebSocket订阅消息
        3. 获取初始订单簿快照

        Args:
            symbol: 交易对符号，例如 'BTCUSDT'
            callback: 订单簿数据回调函数
        """
        super().subscribe_orderbook(symbol, callback)
        if self._ws:
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [f"{symbol.lower()}@depth@{self._orderbook_update_interval}"],
                "id": 1
            }
            asyncio.create_task(self._ws.send(json.dumps(subscribe_msg)))

    def subscribe_trades(self, symbol: str, callback: Callable[[Trade], Union[None, Awaitable[None]]]) -> None:
        """订阅逐笔成交数据

        订阅指定交易对的成交数据，包括：
        1. 注册回调函数
        2. 发送WebSocket订阅消息

        Args:
            symbol: 交易对符号，例如 'BTCUSDT'
            callback: 成交数据回调函数
        """
        super().subscribe_trades(symbol, callback)
        if self._ws:
            # 发送订阅消息
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [f"{symbol.lower()}@trade"],
                "id": 1
            }
            asyncio.create_task(self._ws.send(json.dumps(subscribe_msg)))

    def _merge_orderbook_update_to_snapshot(self, symbol: str, data: dict) -> None:
        """将增量订单簿更新合并到现有的快照中。"""
        orderbook = self._orderbook_snapshot_cache[symbol]

        # 更新买单
        for bid in data['b']:
            price = Decimal(bid[0])
            quantity = Decimal(bid[1])
            if quantity == 0:
                # 删除价格为price的买单
                orderbook.bids = [level for level in orderbook.bids if level.price != price]
            else:
                # 更新或添加买单
                found = False
                for level in orderbook.bids:
                    if level.price == price:
                        level.quantity = quantity
                        found = True
                        break
                if not found:
                    orderbook.bids.append(OrderBookLevel(price, quantity))
                    orderbook.bids.sort(key=lambda x: x.price, reverse=True)

        # 更新卖单
        for ask in data['a']:
            price = Decimal(ask[0])
            quantity = Decimal(ask[1])
            if quantity == 0:
                # 删除价格为price的卖单
                orderbook.asks = [level for level in orderbook.asks if level.price != price]
            else:
                # 更新或添加卖单
                found = False
                for level in orderbook.asks:
                    if level.price == price:
                        level.quantity = quantity
                        found = True
                        break
                if not found:
                    orderbook.asks.append(OrderBookLevel(price, quantity))
                    orderbook.asks.sort(key=lambda x: x.price)

        orderbook.timestamp = data['E']
