"""
WebSocket连接管理器

提供WebSocket连接的通用管理功能，包括：
1. 连接状态管理
2. 自动重连机制
3. 消息路由
4. 资源管理
5. 错误处理
"""

import json
import asyncio
from enum import Enum, auto
from typing import Optional, List, Dict, Any, Protocol, Union, Awaitable, Callable
from dataclasses import dataclass, field
import websockets
from util.logger import get_logger

class ConnectionState(Enum):
    """WebSocket连接状态"""
    DISCONNECTED = auto()  # 未连接
    CONNECTING = auto()    # 正在连接
    CONNECTED = auto()     # 已连接
    RECONNECTING = auto()  # 正在重连
    DISCONNECTING = auto() # 正在断开连接

@dataclass
class ReconnectConfig:
    """重连配置"""
    max_retries: int = 5           # 最大重试次数
    initial_interval: float = 1.0  # 初始重连间隔（秒）
    max_interval: float = 30.0     # 最大重连间隔（秒）
    backoff_factor: float = 2.0    # 退避因子

@dataclass
class ConnectionConfig:
    """连接配置"""
    timeout: float = 30.0          # 连接超时时间（秒）
    ping_interval: float = 20.0    # 心跳间隔（秒）
    ping_timeout: float = 10.0     # 心跳超时时间（秒）
    close_timeout: float = 10.0    # 关闭超时时间（秒）

class WebSocketError(Exception):
    """WebSocket基础异常类"""
    pass

class ConnectionError(WebSocketError):
    """连接相关异常"""
    pass

class MessageError(WebSocketError):
    """消息处理相关异常"""
    pass

class MessageHandler(Protocol):
    """消息处理器协议"""
    async def handle(self, message: dict) -> None:
        """处理WebSocket消息
        
        Args:
            message: WebSocket消息数据
        """
        pass

class MessageRouter:
    """消息路由器
    
    负责将消息分发给注册的处理器
    """
    def __init__(self):
        self._handlers: List[MessageHandler] = []
        
    def add_handler(self, handler: MessageHandler) -> None:
        """添加消息处理器
        
        Args:
            handler: 消息处理器实例
        """
        if handler not in self._handlers:
            self._handlers.append(handler)
            
    def remove_handler(self, handler: MessageHandler) -> None:
        """移除消息处理器
        
        Args:
            handler: 要移除的消息处理器实例
        """
        if handler in self._handlers:
            self._handlers.remove(handler)
            
    async def route(self, message: dict) -> None:
        """路由消息到所有处理器
        
        Args:
            message: 要路由的消息数据
        """
        for handler in self._handlers:
            try:
                await handler.handle(message)
            except Exception as e:
                # 单个处理器失败不影响其他处理器
                get_logger("MessageRouter").error(f"消息处理器执行失败: {e}")

class WebSocketManager:
    """WebSocket连接管理器
    
    负责管理WebSocket连接的生命周期，包括：
    1. 建立和断开连接
    2. 自动重连
    3. 消息路由
    4. 资源管理
    """
    
    def __init__(
        self,
        ws_url: str,
        reconnect_config: Optional[ReconnectConfig] = None,
        connection_config: Optional[ConnectionConfig] = None,
        logger=None
    ):
        """
        初始化WebSocket管理器
        
        Args:
            ws_url: WebSocket服务器URL
            reconnect_config: 重连配置
            connection_config: 连接配置
            logger: 日志记录器
        """
        self._ws_url = ws_url
        self.logger = logger or get_logger("WebSocketManager")
        self.logger.info(f"初始化WebSocket管理器，目标URL: {ws_url}")
        
        # 配置
        self._reconnect_config = reconnect_config or ReconnectConfig()
        self._connection_config = connection_config or ConnectionConfig()
        
        # 状态
        self._state = ConnectionState.DISCONNECTED
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._message_handler_task: Optional[asyncio.Task] = None
        self._reconnect_count = 0
        
        # 消息路由
        self._router = MessageRouter()
        
        # 重连回调
        self._reconnect_callbacks: List[Callable[[], Awaitable[None]]] = []
        
        self.logger.info("WebSocket管理器初始化完成")
        
    @property
    def state(self) -> ConnectionState:
        """获取当前连接状态"""
        return self._state
        
    @property
    def is_connected(self) -> bool:
        """检查是否已连接"""
        return self._state == ConnectionState.CONNECTED
        
    def add_message_handler(self, handler: MessageHandler) -> None:
        """添加消息处理器
        
        Args:
            handler: 消息处理器实例
        """
        self._router.add_handler(handler)
        
    def remove_message_handler(self, handler: MessageHandler) -> None:
        """移除消息处理器
        
        Args:
            handler: 要移除的消息处理器实例
        """
        self._router.remove_handler(handler)
        
    def add_reconnect_callback(self, callback: Callable[[], Awaitable[None]]) -> None:
        """添加重连回调函数
        
        Args:
            callback: 重连成功后的回调函数
        """
        if callback not in self._reconnect_callbacks:
            self._reconnect_callbacks.append(callback)
            
    def remove_reconnect_callback(self, callback: Callable[[], Awaitable[None]]) -> None:
        """移除重连回调函数
        
        Args:
            callback: 要移除的重连回调函数
        """
        if callback in self._reconnect_callbacks:
            self._reconnect_callbacks.remove(callback)
            
    async def connect(self) -> None:
        """建立WebSocket连接并启动消息处理循环"""
        if self._state != ConnectionState.DISCONNECTED:
            raise ConnectionError("WebSocket已经连接或正在连接")
            
        self._state = ConnectionState.CONNECTING
        self.logger.info(f"正在连接到WebSocket服务器: {self._ws_url}")
        
        try:
            self._ws = await websockets.connect(
                self._ws_url,
                ping_interval=self._connection_config.ping_interval,
                ping_timeout=self._connection_config.ping_timeout,
                close_timeout=self._connection_config.close_timeout
            )
            self._state = ConnectionState.CONNECTED
            self._reconnect_count = 0
            self.logger.info("WebSocket连接已建立")
            
            # 启动消息处理循环
            self._message_handler_task = asyncio.create_task(self._handle_messages())
            self.logger.info("消息处理循环已启动")
            
        except Exception as e:
            self._state = ConnectionState.DISCONNECTED
            self.logger.error(f"WebSocket连接失败: {e}")
            raise ConnectionError(f"连接失败: {e}")
            
    async def disconnect(self) -> None:
        """断开WebSocket连接并清理资源"""
        if self._state == ConnectionState.DISCONNECTED:
            return
            
        self._state = ConnectionState.DISCONNECTING
        self.logger.info("正在断开WebSocket连接...")
        
        # 取消消息处理任务
        if self._message_handler_task:
            self.logger.debug("正在取消消息处理任务...")
            self._message_handler_task.cancel()
            try:
                await self._message_handler_task
            except asyncio.CancelledError:
                self.logger.debug("消息处理任务已取消")
            self._message_handler_task = None
            
        # 关闭WebSocket连接
        if self._ws:
            try:
                self.logger.debug("正在关闭WebSocket连接...")
                async with asyncio.timeout(self._connection_config.close_timeout):
                    await self._ws.close()
                self.logger.debug("WebSocket连接已正常关闭")
            except (asyncio.TimeoutError, Exception) as e:
                self.logger.warning(f"WebSocket连接关闭超时或发生错误: {e}，强制关闭连接")
                self._ws.fail_connection()
            finally:
                self._ws = None
                
        self._state = ConnectionState.DISCONNECTED
        self.logger.info("WebSocket连接已完全断开")
        
    async def _handle_messages(self) -> None:
        """处理WebSocket消息的主循环"""
        self.logger.info("开始处理WebSocket消息...")
        
        while self._state in {ConnectionState.CONNECTED, ConnectionState.RECONNECTING}:
            try:
                if not self._ws:
                    self.logger.debug("WebSocket连接不存在，等待重连...")
                    await asyncio.sleep(1)
                    continue
                    
                message = await self._ws.recv()
                self.logger.debug(f"收到WebSocket消息: {message[:200]}...")  # 只打印前200个字符
                data = json.loads(message)
                
                # 路由消息到所有处理器
                await self._router.route(data)
                
            except websockets.exceptions.ConnectionClosed:
                if self._state == ConnectionState.CONNECTED:
                    await self._handle_connection_closed()
            except asyncio.CancelledError:
                self.logger.info("消息处理循环被取消")
                break
            except Exception as e:
                if self._state == ConnectionState.CONNECTED:
                    self.logger.error(f"处理消息时出错: {e}")
                    await self._handle_connection_closed()
                    
    async def _handle_connection_closed(self) -> None:
        """处理连接断开的情况"""
        if self._reconnect_count >= self._reconnect_config.max_retries:
            self.logger.error("达到最大重连次数，停止重连")
            self._state = ConnectionState.DISCONNECTED
            return
            
        self._state = ConnectionState.RECONNECTING
        self._reconnect_count += 1
        
        # 计算重连间隔
        reconnect_interval = min(
            self._reconnect_config.initial_interval * (self._reconnect_config.backoff_factor ** (self._reconnect_count - 1)),
            self._reconnect_config.max_interval
        )
        
        self.logger.warning(f"连接已断开，{reconnect_interval}秒后进行第{self._reconnect_count}次重连...")
        await asyncio.sleep(reconnect_interval)
        
        try:
            self._ws = await websockets.connect(
                self._ws_url,
                ping_interval=self._connection_config.ping_interval,
                ping_timeout=self._connection_config.ping_timeout,
                close_timeout=self._connection_config.close_timeout
            )
            self._state = ConnectionState.CONNECTED
            self.logger.info("重连成功")
            
            # 调用重连回调
            for callback in self._reconnect_callbacks:
                try:
                    await callback()
                except Exception as e:
                    self.logger.error(f"重连回调执行失败: {e}")
                    
        except Exception as e:
            self.logger.error(f"重连失败: {e}")
            self._state = ConnectionState.DISCONNECTED
            
    async def send_message(self, message: dict) -> None:
        """发送WebSocket消息
        
        Args:
            message: 要发送的消息数据
        """
        if not self.is_connected:
            raise ConnectionError("WebSocket未连接，无法发送消息")
            
        try:
            self.logger.debug(f"发送WebSocket消息: {message}")
            await self._ws.send(json.dumps(message))
            self.logger.debug("消息发送成功")
        except Exception as e:
            self.logger.error(f"发送消息失败: {e}")
            raise MessageError(f"发送消息失败: {e}")
            
    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        await self.disconnect() 