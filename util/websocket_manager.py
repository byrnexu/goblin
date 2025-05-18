import json
import asyncio
from typing import Optional, Callable, Awaitable
import websockets
from util.logger import get_logger

class WebSocketManager:
    """WebSocket连接管理器
    
    负责处理WebSocket连接的通用功能，包括：
    1. 建立和断开连接
    2. 消息循环管理
    3. 自动重连机制
    4. 消息发送
    """
    
    def __init__(self, ws_url: str, logger=None):
        self._ws_url = ws_url
        self.logger = logger or get_logger("WebSocketManager")
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._message_handler_task: Optional[asyncio.Task] = None
        self._message_handler: Optional[Callable[[dict], Awaitable[None]]] = None
        
    async def connect(self) -> None:
        """建立WebSocket连接并启动消息处理循环"""
        self.logger.info(f"正在连接到WebSocket服务器: {self._ws_url}")
        self._ws = await websockets.connect(self._ws_url)
        self.logger.info("WebSocket连接已建立")
        self._running = True
        self._message_handler_task = asyncio.create_task(self._handle_messages())
        
    async def disconnect(self) -> None:
        """断开WebSocket连接并清理资源"""
        self.logger.info("正在断开WebSocket连接...")
        self._running = False
        if self._message_handler_task:
            self._message_handler_task.cancel()
            try:
                await self._message_handler_task
            except asyncio.CancelledError:
                pass
            self._message_handler_task = None
            
        if self._ws:
            try:
                async with asyncio.timeout(3):
                    await self._ws.close()
            except (asyncio.TimeoutError, Exception):
                self._ws.fail_connection()
            finally:
                self._ws = None
        self.logger.info("WebSocket连接已断开")
                
    async def _handle_messages(self) -> None:
        """处理WebSocket消息的主循环"""
        self.logger.info("开始处理WebSocket消息...")
        while self._running:
            try:
                if not self._ws:
                    await asyncio.sleep(1)
                    continue
                    
                message = await self._ws.recv()
                self.logger.debug(f"收到WebSocket消息: {message[:200]}...")  # 只打印前200个字符
                data = json.loads(message)
                
                if self._message_handler:
                    self.logger.debug("正在调用消息处理器...")
                    await self._message_handler(data)
                    self.logger.debug("消息处理器调用完成")
                else:
                    self.logger.warning("没有设置消息处理器")
                    
            except websockets.exceptions.ConnectionClosed:
                if self._running:
                    self.logger.warning("连接已断开，正在重连...")
                    await asyncio.sleep(1)
                    try:
                        self._ws = await websockets.connect(self._ws_url)
                        self.logger.info("重连成功")
                        if hasattr(self, 'on_reconnect'):
                            await self.on_reconnect()
                    except Exception as e:
                        self.logger.error(f"重连失败: {e}")
            except asyncio.CancelledError:
                self.logger.info("消息处理循环被取消")
                break
            except Exception as e:
                if self._running:
                    self.logger.error(f"处理消息时出错: {e}")
                    await asyncio.sleep(1)
                    try:
                        self._ws = await websockets.connect(self._ws_url)
                        self.logger.info("重连成功")
                        if hasattr(self, 'on_reconnect'):
                            await self.on_reconnect()
                    except Exception as e:
                        self.logger.error(f"重连失败: {e}")
                        
    async def send_message(self, message: dict) -> None:
        """发送WebSocket消息"""
        if self._ws:
            self.logger.debug(f"发送WebSocket消息: {message}")
            await self._ws.send(json.dumps(message))
        else:
            self.logger.warning("WebSocket未连接，无法发送消息")
            
    def set_message_handler(self, handler: Callable[[dict], Awaitable[None]]) -> None:
        """设置消息处理器"""
        self.logger.info("设置消息处理器")
        self._message_handler = handler 