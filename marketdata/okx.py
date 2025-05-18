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
from util.websocket_manager import WebSocketManager

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
        super().__init__(config, market_type)

        assert market_type in ('spot', 'perp_usdt', 'perp_coin')

        self._orderbook_depth_limit = config.ORDERBOOK_DEPTH_LIMIT

        # 根据市场类型选择对应的symbol转换适配器
        self._symbol_adapter = f"okx_{self._market_type}"

        # 存储每个交易对的订单簿数据
        self._orderbook_snapshot_cache: Dict[str, OrderBook] = {}

        # 用于WebSocket请求的唯一ID
        self._next_request_id = 1

    def get_ws_url(self) -> str:
        """获取WebSocket URL
        
        OKX使用单一的WebSocket URL，而不是按市场类型区分
        
        Returns:
            str: OKX WebSocket URL
        """
        return self._config.WS_URL

    async def connect(self) -> None:
        """连接到OKX WebSocket服务器

        建立WebSocket连接并启动消息处理循环
        同时创建HTTP会话用于REST API请求
        """
        await self._ws_manager.connect()
        self.resubscribe_all()

    async def disconnect(self) -> None:
        """断开与OKX WebSocket服务器的连接

        1. 停止消息处理循环
        2. 取消消息处理任务
        3. 关闭WebSocket连接
        4. 关闭HTTP会话
        """
        await self._ws_manager.disconnect()

    async def _handle_messages(self, data: dict) -> None:
        """处理WebSocket消息

        持续接收并处理WebSocket消息，包括：
        1. 订单簿更新消息
        2. 成交消息
        3. 错误处理
        4. 自动重连
        """
        # 处理具体的消息逻辑
        if 'arg' in data and 'channel' in data['arg']:
            if data['channel']['arg'] == 'sprd-bbo-tbt' or data['channel']['arg'] == 'sprd-books5':
                await self._handle_orderbook(data)
            elif data['channel']['arg'] == 'sprd-books-l2-tbt':
                await self._handle_orderbook_update(data)
        elif 'arg' in data and 'channel' in data['arg']:
            if data['channel']['arg'] == 'sprd-public-trades':
                await self._handle_trade(data)
        if 'event' in data:
            self._handle_subscription_event(data)

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
        if self._ws_manager._ws:
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
            self.logger.info(f"开始订阅行情: {subscribe_msg}")
            asyncio.create_task(self._ws_manager.send_message(subscribe_msg))

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
