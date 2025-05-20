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

@dataclass
class OrderBookLevel:
    """订单簿价格档位

    表示订单簿中的一个价格档位，包含价格和数量信息
    """
    price: Decimal  # 价格
    quantity: Decimal  # 数量

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
        reconnect_config = ReconnectConfig(
            max_retries=5,
            initial_interval=1.0,
            max_interval=30.0,
            backoff_factor=2.0
        )
        connection_config = ConnectionConfig(
            timeout=30.0,
            ping_interval=20.0,
            ping_timeout=10.0,
            close_timeout=10.0
        )
        self._ws_manager: WebSocketManager = WebSocketManager(
            self.get_ws_url(),
            reconnect_config=reconnect_config,
            connection_config=connection_config,
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

    @abstractmethod
    async def connect(self) -> None:
        """连接到交易所

        建立与交易所的连接，包括WebSocket连接等
        具体的实现类需要实现这个方法
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """断开与交易所的连接

        关闭所有连接，释放资源
        具体的实现类需要实现这个方法
        """
        pass

    def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBook], Union[None, Awaitable[None]]]) -> None:
        """订阅订单簿数据

        当指定交易对的订单簿数据更新时，会调用提供的回调函数

        Args:
            symbol: 交易对符号，例如 'BTCUSDT'
            callback: 订单簿数据回调函数，可以是同步或异步函数
        """
        self._orderbook_manager.subscribe(symbol, callback)

    def unsubscribe_orderbook(self, symbol: str, callback: Optional[Callable[[OrderBook], Union[None, Awaitable[None]]]] = None) -> None:
        """取消订阅订单簿数据

        取消指定交易对的订单簿数据订阅

        Args:
            symbol: 交易对符号
            callback: 要取消的回调函数，如果为None则取消所有回调
        """
        self._orderbook_manager.unsubscribe(symbol, callback)

    def subscribe_trades(self, symbol: str, callback: Callable[[Trade], Union[None, Awaitable[None]]]) -> None:
        """订阅成交数据

        当指定交易对有新的成交时，会调用提供的回调函数

        Args:
            symbol: 交易对符号
            callback: 成交数据回调函数，可以是同步或异步函数
        """
        self._trade_manager.subscribe(symbol, callback)

    def unsubscribe_trades(self, symbol: str, callback: Optional[Callable[[Trade], Union[None, Awaitable[None]]]] = None) -> None:
        """取消订阅成交数据

        取消指定交易对的成交数据订阅

        Args:
            symbol: 交易对符号
            callback: 要取消的回调函数，如果为None则取消所有回调
        """
        self._trade_manager.unsubscribe(symbol, callback)

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

    def resubscribe_all(self) -> None:
        """重新订阅所有已订阅的交易对数据"""
        # 由于事件管理器已经保存了所有订阅信息，这里不需要做任何事情
        pass

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
        self.resubscribe_all()
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
