import json
import asyncio
from typing import Dict, List, Optional, Callable, Union, Awaitable
from decimal import Decimal
import websockets
import aiohttp
from .base import MarketDataBase, OrderBook, OrderBookLevel, Trade

class BinanceMarketData(MarketDataBase):
    """币安交易所市场数据实现

    实现了币安交易所的WebSocket API，提供实时市场数据订阅功能。
    包括：
    1. 订单簿数据订阅和更新
    2. 逐笔成交数据订阅
    3. 自动重连机制
    4. 订单簿快照获取
    """

    def __init__(self):
        super().__init__()
        # WebSocket API地址
        self._ws_url = "wss://stream.binance.com:9443/ws"
        # REST API地址
        self._rest_url = "https://api.binance.com/api/v3"
        # WebSocket连接对象
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        # 存储每个交易对的订单簿数据
        self._orderbooks: Dict[str, OrderBook] = {}
        # 控制消息处理循环的运行状态
        self._running = False
        # 消息处理任务
        self._message_handler_task: Optional[asyncio.Task] = None
        # HTTP会话，用于REST API请求
        self._session: Optional[aiohttp.ClientSession] = None
        # 缓存原始数据
        self._raw_data_cache: Dict[str, List[dict]] = {}

    async def connect(self) -> None:
        """连接到币安WebSocket服务器

        建立WebSocket连接并启动消息处理循环
        同时创建HTTP会话用于REST API请求
        """
        self._ws = await websockets.connect(self._ws_url)
        self._running = True
        self._message_handler_task = asyncio.create_task(self._handle_messages())
        self._session = aiohttp.ClientSession()

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
        timestamp = data['E']

        # 缓存原始数据
        if symbol not in self._raw_data_cache:
            self._raw_data_cache[symbol] = []
        self._raw_data_cache[symbol].append(data)

        # 获取或创建订单簿
        if symbol not in self._orderbooks:
            # 首次订阅时，需要获取完整的订单簿快照
            await self._get_orderbook_snapshot(symbol)
            return

        orderbook = self._orderbooks[symbol]

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

        orderbook.timestamp = timestamp
        await self._notify_orderbook(orderbook)

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
                "limit": 100  # 获取100档深度
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
                    self._orderbooks[symbol] = orderbook

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
            # 发送订阅消息
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [f"{symbol.lower()}@depth"],
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
