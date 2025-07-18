"""
市场数据基础接口模块

本模块定义了市场数据订阅的基本接口，包括：
1. 连接和断开连接
2. 订阅和取消订阅订单簿数据
3. 订阅和取消订阅成交数据
4. 数据更新通知机制
"""

from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional, Union, Awaitable, Any
from dataclasses import dataclass, field
from decimal import Decimal
import asyncio
from sortedcontainers import SortedDict
from util.logger import get_logger
from util.websocket_manager import WebSocketManager, MessageHandler, ReconnectConfig, ConnectionConfig
from .event_manager import EventManager
from .types import OrderBook, OrderBookLevel, Trade, Market, MarketType
import aiohttp

class MarketDataMessageHandler(MessageHandler):
    """市场数据消息处理器

    负责处理WebSocket消息并更新本地状态
    """
    def __init__(self, market_data):
        self._market_data = market_data

    async def handle(self, message: dict) -> None:
        """处理WebSocket消息

        Args:
            message: WebSocket消息数据
        """
        await self._market_data._handle_messages(message)

class MarketDataBase(ABC):
    """市场数据基础接口类

    定义了市场数据订阅的基本接口，包括：
    1. 连接和断开连接
    2. 订阅和取消订阅订单簿数据
    3. 订阅和取消订阅成交数据
    4. 数据更新通知机制

    具体的交易所实现类需要继承这个基类并实现其抽象方法
    """

    def __init__(self, config, market_type: MarketType = MarketType.SPOT):
        """
        初始化市场数据基础类

        Args:
            config: 交易所配置对象
            market_type: 市场类型，使用 MarketType 枚举
        """
        # 使用事件管理器管理回调
        self._orderbook_manager: EventManager[OrderBook] = EventManager()
        self._trade_manager: EventManager[Trade] = EventManager()

        # 存储交易所配置
        self._config: Any = config
        # 市场
        self._market: Market = config.MARKET
        # 市场类型
        self._market_type: MarketType = market_type
        # 日志记录器
        self.logger = get_logger(f"{self.__class__.__name__}")

        # 创建WebSocket管理器
        self._ws_manager: WebSocketManager = WebSocketManager(
            self.get_ws_url(),
            reconnect_config=self._config.WS_RECONNECT_CONFIG,
            connection_config=self._config.WS_CONNECTION_CONFIG,
            logger=self.logger
        )

        # 设置消息处理器
        self._ws_manager.add_message_handler(MarketDataMessageHandler(self))
        # 设置重连回调
        self._ws_manager.add_reconnect_callback(self._handle_reconnect)

        self.logger.info(f"市场数据服务初始化完成")
        # 存储每个交易对的订单簿快照
        self._orderbook_snapshot_cache: Dict[str, OrderBook] = {}
        # 运行状态标志
        self._running: bool = False
        # HTTP会话对象，用于REST API请求
        self._session: Optional[aiohttp.ClientSession] = None

    def get_ws_url(self) -> str:
        """获取WebSocket URL

        返回当前市场类型对应的WebSocket URL。
        默认实现从配置对象的 WS_URLS 字典中获取。
        如果交易所的配置结构不同，子类可以重写此方法。

        Returns:
            str: WebSocket URL
        """
        return self._config.WS_URLS[self._market_type]

    def _symbol_adapter(self):
        """
        获取当前合约类型对应的symbol适配器名
        :return: 'binance_perp_usdt' 或 'binance_perp_coin' 或 'binance_spot'
        """
        return f"{self._market.value}_{self._market_type.value}"

    @abstractmethod
    async def _handle_messages(self, data: dict) -> None:
        """处理WebSocket消息

        处理从WebSocket接收到的消息

        Args:
            data: WebSocket消息数据
        """
        pass

    async def connect(self) -> None:
        """连接到交易所

        建立与交易所的连接，包括WebSocket连接等。
        默认实现会：
        1. 如果配置中包含REST_URLS，创建HTTP会话
        2. 建立WebSocket连接
        3. 设置运行状态
        4. 记录连接日志

        子类可以重写此方法以添加额外的连接逻辑。
        """
        self.logger.info("开始建立市场数据连接...")

        # 如果配置中包含REST_URLS，创建HTTP会话
        if hasattr(self._config, 'REST_URLS'):
            self._session = aiohttp.ClientSession()
            self.logger.info("已创建HTTP会话")

        await self._ws_manager.connect()
        self._running = True
        self.logger.info("市场数据连接建立完成")

    async def disconnect(self) -> None:
        """断开与交易所的连接

        关闭所有连接，释放资源。
        默认实现会：
        1. 设置停止标志
        2. 断开WebSocket连接
        3. 如果存在HTTP会话，关闭它
        4. 记录断开日志

        子类可以重写此方法以添加额外的断开连接逻辑。
        """
        self.logger.info("开始断开市场数据连接...")
        self._running = False
        await self._ws_manager.disconnect()

        # 如果存在HTTP会话，关闭它
        if self._session:
            await self._session.close()
            self._session = None
            self.logger.info("已关闭HTTP会话")

        self.logger.info("市场数据连接已断开")

    @abstractmethod
    async def _build_orderbook_subscription_message(self, symbol: str) -> dict:
        """
        构建订单簿订阅消息
        
        Args:
            symbol: 交易对符号
            
        Returns:
            dict: 订阅消息内容
        """
        pass

    @abstractmethod
    async def _build_trade_subscription_message(self, symbol: str) -> dict:
        """
        构建成交订阅消息
        
        Args:
            symbol: 交易对符号
            
        Returns:
            dict: 订阅消息内容
        """
        pass

    async def _send_subscription_message(self, subscribe_msg: dict) -> None:
        """
        发送订阅消息到WebSocket
        
        Args:
            subscribe_msg: 订阅消息内容
        """
        self.logger.info(f"发送订阅请求: {subscribe_msg}")
        await self._ws_manager.send_message(subscribe_msg)

    async def _send_orderbook_subscription(self, symbol: str) -> None:
        """
        发送订单簿订阅请求
        
        Args:
            symbol: 交易对符号
        """
        subscribe_msg = await self._build_orderbook_subscription_message(symbol)
        await self._send_subscription_message(subscribe_msg)

    async def _send_trade_subscription(self, symbol: str) -> None:
        """
        发送成交订阅请求
        
        Args:
            symbol: 交易对符号
        """
        subscribe_msg = await self._build_trade_subscription_message(symbol)
        await self._send_subscription_message(subscribe_msg)

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
        self._orderbook_manager.subscribe(symbol, callback)
        if self._ws_manager.is_connected:
            asyncio.create_task(self._send_orderbook_subscription(symbol))

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
        self._trade_manager.subscribe(symbol, callback)
        if self._ws_manager.is_connected:
            asyncio.create_task(self._send_trade_subscription(symbol))

    def get_all_orderbook_subscribed_symbols(self) -> List[str]:
        """获取所有已订阅订单簿的交易对

        Returns:
            List[str]: 已订阅订单簿的交易对列表
        """
        return self._orderbook_manager.get_subscribed_symbols()

    def get_all_trade_subscribed_symbols(self) -> List[str]:
        """获取所有已订阅成交的交易对

        Returns:
            List[str]: 已订阅成交的交易对列表
        """
        return self._trade_manager.get_subscribed_symbols()

    async def resubscribe_all(self) -> None:
        """
        重新订阅所有已订阅的交易对

        在WebSocket重连后调用，重新发送所有订阅请求。
        使用当前已订阅的交易对信息，而不是存储的旧请求。
        """
        self.logger.info("开始重新订阅所有交易对...")

        # 重新订阅所有订单簿
        for symbol in self._orderbook_manager.get_subscribed_symbols():
            self.logger.info(f"重新订阅{symbol}的订单簿数据")
            await self._send_orderbook_subscription(symbol)

        # 重新订阅所有成交
        for symbol in self._trade_manager.get_subscribed_symbols():
            self.logger.info(f"重新订阅{symbol}的成交数据")
            await self._send_trade_subscription(symbol)

        self.logger.info("所有交易对重新订阅完成")

    async def _handle_reconnect(self) -> None:
        """
        WebSocket重连后的处理函数

        默认实现会：
        1. 清空订单簿快照缓存
        2. 重新订阅所有交易对
        """
        self.logger.info("WebSocket重连成功，开始重新订阅...")
        # 清空订单簿快照缓存
        self._orderbook_snapshot_cache.clear()
        # 重新订阅所有交易对
        await self.resubscribe_all()
        self.logger.info("重新订阅完成")

    async def _notify_orderbook(self, orderbook: OrderBook) -> None:
        """通知订单簿更新

        将订单簿更新通知给所有订阅者

        Args:
            orderbook: 更新后的订单簿对象
        """
        await self._orderbook_manager.notify(orderbook.symbol, orderbook)

    async def _notify_trade(self, trade: Trade) -> None:
        """通知成交更新

        将成交更新通知给所有订阅者

        Args:
            trade: 成交对象
        """
        await self._trade_manager.notify(trade.symbol, trade)
