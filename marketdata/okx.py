import json
import asyncio
from typing import Dict, List, Optional, Callable, Union, Awaitable
from decimal import Decimal
import websockets
import aiohttp
from .base import MarketDataBase, OrderBook, OrderBookLevel, Trade
from .config import OkxConfig
from sortedcontainers import SortedDict
from util.logger import get_logger
from util.symbol_convert import to_exchange, from_exchange

class OkxMarketData(MarketDataBase):
    """OKX交易所市场数据实现

    实现了OKX交易所的WebSocket API，提供实时市场数据订阅功能。
    包括：
    1. 订单簿数据订阅和更新
    2. 逐笔成交数据订阅
    3. 自动重连机制
    4. 订单簿快照获取
    """

    def __init__(self, config: OkxConfig = OkxConfig(), market_type: str = "spot"):
        """
        初始化OKX市场数据客户端

        Args:
            config: OKX配置对象
            market_type: 市场类型，可选值：
                - "spot": 现货市场
                - "perp_usdt": USDT本位永续合约
                - "perp_coin": 币本位永续合约
        """
        super().__init__()

        self._market_type = market_type

        # 将 market_type 转换为驼峰格式
        market_type_camel = ''.join(word.capitalize() for word in market_type.split('_'))
        self.logger = get_logger(f"Okx{market_type_camel}MarketData")

        self._config = config
        self._ws_url = config.WS_URL
        self._orderbook_depth_limit = config.ORDERBOOK_DEPTH_LIMIT

        # 根据市场类型选择对应的symbol转换适配器
        self._symbol_adapter = f"okx_{self._market_type}"

        # WebSocket连接对象
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

        # 控制消息处理循环的运行状态
        self._running = False
        # 消息处理任务
        self._message_handler_task: Optional[asyncio.Task] = None

        # 存储每个交易对的订单簿数据
        self._orderbook_snapshot_cache: Dict[str, OrderBook] = {}

        # 用于WebSocket请求的唯一ID
        self._next_request_id = 1

    async def connect(self) -> None:
        """连接到OKX WebSocket服务器

        建立WebSocket连接并启动消息处理循环
        同时创建HTTP会话用于REST API请求
        """
        self._ws = await websockets.connect(self._ws_url)

        self._running = True
        self._message_handler_task = asyncio.create_task(self._handle_messages())

        self.resubscribe_all()

    async def disconnect(self) -> None:
        """断开与OKX WebSocket服务器的连接

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


    async def _handle_messages(self) -> None:
        """处理WebSocket消息

        持续接收并处理WebSocket消息，包括：
        1. 订单簿更新消息
        2. 成交消息
        3. 错误处理
        4. 自动重连
        """
        while self._running:
            try:
                if not self._ws:
                    await asyncio.sleep(1)
                    continue

                message = await self._ws.recv()
                data = json.loads(message)

                # 处理订单簿数据
                # 获取Spread深度数据。可用频道有：

                # sprd-bbo-tbt: 首次推1档快照数据，以后定量推送，每10毫秒当1档快照数据有变化推送一次1档数据
                # sprd-books5: 首次推5档快照数据，以后定量推送，每100毫秒当5档快照数据有变化推送一次5档数据
                # sprd-books-l2-tbt: 首次推400档快照数据，以后增量推送，每10毫秒推送一次变化的数据
                # 单个连接、交易产品维度，深度频道的推送顺序固定为：sprd-bbo-tbt -> sprd-books-l2-tbt -> sprd-books5
                if 'arg' in data and 'channel' in data['arg']:
                    if data['channel']['arg'] == 'sprd-bbo-tbt' or  \
                        data['channel']['arg'] == 'sprd-books5'  :
                        await self._handle_orderbook(data)
                    elif data['channel']['arg'] == 'sprd-books-l2-tbt':
                        await self._handle_orderbook_update(data)
                # 处理成交数据
                elif 'arg' in data and 'channel' in data['arg']:
                    if data['channel']['arg'] == 'sprd-public-trades':
                        await self._handle_trade(data)
                # 处理订阅/退订结果消息
                if 'event' in data:
                    self._handle_subscription_event(data)
            except websockets.exceptions.ConnectionClosed as e:
                if self._running:
                    self.logger.warning("连接已断开，正在重连...")
                    await asyncio.sleep(1)
                    try:
                        self._ws = await websockets.connect(self._ws_url)
                        self.resubscribe_all()
                    except Exception as e:
                        self.logger.error(f"重连失败: {e}")
            except asyncio.CancelledError:
                # 任务被取消，正常退出
                break
            except Exception as e:
                if self._running:
                    self.logger.error(f"处理消息时出错: {e}")
                    await asyncio.sleep(1)
                    try:
                        self._ws = await websockets.connect(self._ws_url)
                        self.resubscribe_all()
                    except Exception as e:
                        self.logger.error(f"重连失败: {e}")

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
        """处理订单簿更新消息

        处理OKX WebSocket的订单簿更新消息，包括：
        1. 更新买单
        2. 更新卖单
        3. 维护订单簿状态
        4. 通知订阅者

        Args:
            data: 订单簿更新数据，包含买单和卖单的更新信息
        """
        # TODO: Implement OKX orderbook update handling
        pass

    async def _handle_trade(self, data: dict) -> None:
        """处理成交消息

        处理OKX WebSocket的成交消息，转换为内部Trade对象并通知订阅者

        Args:
            data: 成交数据，包含价格、数量、方向等信息
        """
        # TODO: Implement OKX trade handling
        pass

    def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBook], Union[None, Awaitable[None]]]) -> None:
        """订阅订单簿数据

        订阅指定交易对的订单簿数据，包括：
        1. 注册回调函数
        2. 发送WebSocket订阅消息
        3. 获取初始订单簿快照

        Args:
            symbol: 交易对符号，例如 'BTC/USDT'
            callback: 订单簿数据回调函数
        """
        super().subscribe_orderbook(symbol, callback)
        if self._ws:
            exchange_symbol = to_exchange(symbol, self._symbol_adapter)
            subscribe_msg = {
                "op": "subscribe",
                "args": [
                    {
                        "channel": "sprd-books-l2-tbt",  # 默认订阅400档增量
                        "sprdId": "BTC-USDT_BTC-USDT-SWAP"
                    }
                ]
            }
            self.logger.info(f"发送订单簿订阅请求: {subscribe_msg}")
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
        # TODO: Implement OKX trades subscription
        pass

    def resubscribe_all(self):
        """重新订阅所有已订阅的交易对数据"""
        # TODO: Implement resubscription logic
        pass

    def _handle_subscription_event(self, data: dict) -> None:
        """处理订阅、退订、错误事件消息并打印日志"""
        event = data['event']
        arg = data.get('arg', {})
        channel = arg.get('channel', '')
        sprd_id = arg.get('sprdId', '')
        if event == 'subscribe':
            self.logger.info(f"订阅成功: channel={channel}, sprdId={sprd_id}")
        elif event == 'unsubscribe':
            self.logger.info(f"取消订阅成功: channel={channel}, sprdId={sprd_id}")
        elif event == 'error':
            msg = data.get('msg', '')
            code = data.get('code', '')
            self.logger.error(f"订阅失败: channel={channel}, sprdId={sprd_id}, code={code}, msg={msg}")
