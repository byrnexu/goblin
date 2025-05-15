from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional, Union, Awaitable, Any
from dataclasses import dataclass, field
from decimal import Decimal
import asyncio
from sortedcontainers import SortedDict

@dataclass
class OrderBookLevel:
    """订单簿价格档位
    
    表示订单簿中的一个价格档位，包含价格和数量信息
    """
    price: Decimal  # 价格
    quantity: Decimal  # 数量

@dataclass
class OrderBook:
    """订单簿数据结构
    
    包含交易对的完整订单簿信息，包括买单和卖单
    """
    symbol: str  # 交易对符号，例如 'BTCUSDT'
    bids: SortedDict  # 价格降序
    asks: SortedDict  # 价格升序
    timestamp: int  # 毫秒时间戳，表示订单簿更新的时间
    aux_data: Dict[str, Any] = field(default_factory=dict)  # 通用辅助数据字段

@dataclass
class Trade:
    """成交数据结构
    
    表示一笔成交记录
    """
    symbol: str  # 交易对符号
    price: Decimal  # 成交价格
    quantity: Decimal  # 成交数量
    side: str  # 成交方向：'buy'表示买方成交，'sell'表示卖方成交
    timestamp: int  # 毫秒时间戳，表示成交时间
    trade_id: str  # 成交ID，用于唯一标识一笔成交

class MarketDataBase(ABC):
    """市场数据基础接口类
    
    定义了市场数据订阅的基本接口，包括：
    1. 连接和断开连接
    2. 订阅和取消订阅订单簿数据
    3. 订阅和取消订阅成交数据
    4. 数据更新通知机制
    
    具体的交易所实现类需要继承这个基类并实现其抽象方法
    """
    
    def __init__(self):
        # 存储每个交易对的订单簿回调函数列表
        self._orderbook_callbacks: Dict[str, List[Callable[[OrderBook], Union[None, Awaitable[None]]]]] = {}
        # 存储每个交易对的成交回调函数列表
        self._trade_callbacks: Dict[str, List[Callable[[Trade], Union[None, Awaitable[None]]]]] = {}
    
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
        if symbol not in self._orderbook_callbacks:
            self._orderbook_callbacks[symbol] = []
        if callback not in self._orderbook_callbacks[symbol]:
            self._orderbook_callbacks[symbol].append(callback)
    
    def subscribe_trades(self, symbol: str, callback: Callable[[Trade], Union[None, Awaitable[None]]]) -> None:
        """订阅逐笔成交数据
        
        当指定交易对有新的成交时，会调用提供的回调函数
        
        Args:
            symbol: 交易对符号，例如 'BTCUSDT'
            callback: 成交数据回调函数，可以是同步或异步函数
        """
        if symbol not in self._trade_callbacks:
            self._trade_callbacks[symbol] = []
        if callback not in self._trade_callbacks[symbol]:
            self._trade_callbacks[symbol].append(callback)
    
    def unsubscribe_orderbook(self, symbol: str, callback: Optional[Callable[[OrderBook], Union[None, Awaitable[None]]]] = None) -> None:
        """取消订阅订单簿数据
        
        取消指定交易对的订单簿数据订阅
        
        Args:
            symbol: 交易对符号
            callback: 要取消的回调函数，如果为None则取消该交易对的所有回调
        """
        if symbol in self._orderbook_callbacks:
            if callback is None:
                del self._orderbook_callbacks[symbol]
            else:
                self._orderbook_callbacks[symbol].remove(callback)
    
    def unsubscribe_trades(self, symbol: str, callback: Optional[Callable[[Trade], Union[None, Awaitable[None]]]] = None) -> None:
        """取消订阅逐笔成交数据
        
        取消指定交易对的成交数据订阅
        
        Args:
            symbol: 交易对符号
            callback: 要取消的回调函数，如果为None则取消该交易对的所有回调
        """
        if symbol in self._trade_callbacks:
            if callback is None:
                del self._trade_callbacks[symbol]
            else:
                self._trade_callbacks[symbol].remove(callback)
    
    async def _notify_orderbook(self, orderbook: OrderBook) -> None:
        """通知订单簿数据更新
        
        当订单簿数据更新时，调用所有注册的回调函数
        
        Args:
            orderbook: 更新后的订单簿数据
        """
        if orderbook.symbol in self._orderbook_callbacks:
            for callback in self._orderbook_callbacks[orderbook.symbol]:
                result = callback(orderbook)
                if asyncio.iscoroutine(result):
                    await result
    
    async def _notify_trade(self, trade: Trade) -> None:
        """通知成交数据更新
        
        当有新的成交时，调用所有注册的回调函数
        
        Args:
            trade: 新的成交数据
        """
        if trade.symbol in self._trade_callbacks:
            for callback in self._trade_callbacks[trade.symbol]:
                result = callback(trade)
                if asyncio.iscoroutine(result):
                    await result

    def get_all_orderbook_subscribed_symbols(self):
        return list(self._orderbook_callbacks.keys())

    def get_all_trade_subscribed_symbols(self):
        return list(self._trade_callbacks.keys())

    def resubscribe_all(self):
        for symbol in self.get_all_orderbook_subscribed_symbols():
            for callback in self._orderbook_callbacks[symbol]:
                self.subscribe_orderbook(symbol, callback)
        for symbol in self.get_all_trade_subscribed_symbols():
            for callback in self._trade_callbacks[symbol]:
                self.subscribe_trades(symbol, callback) 