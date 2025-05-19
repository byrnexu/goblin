"""
事件管理器模块

本模块实现了一个通用的事件管理器，用于处理市场数据系统中的事件通知。
使用观察者模式实现，支持同步和异步回调函数。

主要功能：
1. 管理事件订阅和取消订阅
2. 处理事件通知
3. 支持多种事件类型（通过泛型实现）
4. 支持同步和异步回调函数

使用示例：
    # 创建订单簿事件管理器
    orderbook_manager = EventManager[OrderBook]()
    
    # 订阅事件
    orderbook_manager.subscribe("BTCUSDT", callback_function)
    
    # 发送通知
    await orderbook_manager.notify("BTCUSDT", orderbook_data)
"""

from typing import Dict, List, Callable, Union, Awaitable, Any, TypeVar, Generic
from dataclasses import dataclass, field
import asyncio

# 定义泛型类型变量，用于支持不同类型的事件数据
T = TypeVar('T')

@dataclass
class EventManager(Generic[T]):
    """事件管理器，使用观察者模式管理回调函数
    
    用于管理特定类型事件（如订单簿更新、成交等）的回调函数。
    支持同步和异步回调函数，可以处理多种类型的事件数据。

    Attributes:
        _callbacks: 存储每个交易对的所有回调函数
            key: 交易对符号
            value: 该交易对的所有回调函数列表

    Type Parameters:
        T: 事件数据类型，例如OrderBook或Trade
    """
    _callbacks: Dict[str, List[Callable[[T], Union[None, Awaitable[None]]]]] = field(default_factory=dict)

    def subscribe(self, symbol: str, callback: Callable[[T], Union[None, Awaitable[None]]]) -> None:
        """订阅特定交易对的事件
        
        为指定的交易对添加一个回调函数。当该交易对有事件发生时，
        所有注册的回调函数都会被调用。

        Args:
            symbol: 交易对符号，例如 'BTCUSDT'
            callback: 回调函数，可以是同步或异步函数
                    回调函数接收一个类型为T的参数，表示事件数据

        Note:
            - 同一个回调函数不会重复添加
            - 回调函数可以是同步或异步的
        """
        if symbol not in self._callbacks:
            self._callbacks[symbol] = []
        if callback not in self._callbacks[symbol]:
            self._callbacks[symbol].append(callback)

    def unsubscribe(self, symbol: str, callback: Callable[[T], Union[None, Awaitable[None]]] = None) -> None:
        """取消订阅特定交易对的事件
        
        移除指定交易对的一个或多个回调函数。

        Args:
            symbol: 交易对符号
            callback: 要取消的回调函数
                    如果为None，则取消该交易对的所有回调

        Note:
            - 如果callback为None，将移除该交易对的所有回调
            - 如果callback不在回调列表中，不会产生错误
        """
        if symbol in self._callbacks:
            if callback is None:
                del self._callbacks[symbol]
            else:
                self._callbacks[symbol].remove(callback)

    async def notify(self, symbol: str, data: T) -> None:
        """通知所有订阅者
        
        当事件发生时，调用所有注册的回调函数。

        Args:
            symbol: 交易对符号
            data: 要通知的数据，类型为T

        Note:
            - 如果回调函数是异步的，会等待其完成
            - 如果回调函数执行出错，不会影响其他回调的执行
            - 如果交易对没有注册的回调，不会执行任何操作
        """
        if symbol in self._callbacks:
            for callback in self._callbacks[symbol]:
                result = callback(data)
                if asyncio.iscoroutine(result):
                    await result

    def get_subscribed_symbols(self) -> List[str]:
        """获取所有已订阅的交易对
        
        返回所有已注册回调的交易对列表。

        Returns:
            List[str]: 已订阅的交易对列表

        Note:
            - 返回的是当前所有有回调注册的交易对
            - 如果某个交易对的所有回调都被取消，该交易对不会出现在列表中
        """
        return list(self._callbacks.keys())

    def clear(self) -> None:
        """清空所有订阅
        
        移除所有交易对的所有回调函数。

        Note:
            - 此操作不可逆
            - 调用后所有交易对的回调都会被移除
        """
        self._callbacks.clear() 