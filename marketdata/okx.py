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
        self.logger.info(f"初始化OKX市场数据服务 market {market_type.value} ...")

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
            self.logger.info(f"收到订单簿更新消息: {data['arg']['instId']}")
            await self._handle_orderbook_update(data)
        elif 'arg' in data and data['arg'].get('channel') in {'bbo-tbt', 'books5'}:
            self.logger.info(f"收到订单簿快照消息: {data['arg']['instId']}")
            await self._handle_orderbook_snapshot(data)
        elif 'arg' in data and data['arg']['channel'] == 'trades':
            self.logger.info(f"收到成交消息: {data['arg']['instId']}")
            await self._handle_trade(data)

    async def _handle_orderbook_update(self, data: dict) -> None:
        # TODO: Implement OKX orderbook update handling
        pass

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

    async def _handle_trade(self, data: dict) -> None:
        pass

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
            # 定义深度限制到频道名称的映射
            CHANNEL_MAPPING = { 1: "bbo-tbt", 5: "books5", 400: "books" }
            # 构建订阅消息
            subscribe_msg = {
                "op": "subscribe",
                "args": [{
                    "channel": CHANNEL_MAPPING.get(
                        self._config.ORDERBOOK_DEPTH_LIMIT.get(self._market_type, 400),  # 默认值400
                        "books"  # 万一中途添加了新的MarketType，提供默认频道
                    ),
                    "instId": exchange_symbol
                }]
            }
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
            self.logger.info(f"发送成交订阅请求: {subscribe_msg}")
            asyncio.create_task(self._ws_manager.send_message(subscribe_msg))

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
