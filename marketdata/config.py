from typing import Dict, Any
from .types import MarketType

class BaseConfig:
    """所有交易所通用的基础配置，可扩展"""
    pass

class BinanceConfig(BaseConfig):
    WS_URLS: Dict[MarketType, str] = {
        MarketType.SPOT: "wss://stream.binance.com:9443/ws",
        MarketType.PERP_USDT: "wss://fstream.binance.com/ws",
        MarketType.PERP_COIN: "wss://dstream.binance.com/ws"
    }
    REST_URLS: Dict[MarketType, str] = {
        MarketType.SPOT: "https://api.binance.com/api/v3",
        MarketType.PERP_USDT: "https://fapi.binance.com/fapi/v1",
        MarketType.PERP_COIN: "https://dapi.binance.com/dapi/v1"
    }
    ORDERBOOK_DEPTH_LIMIT: Dict[MarketType, int] = {
        MarketType.SPOT: 5000, # 最大5000
        MarketType.PERP_USDT: 1000, # 5 10 20 50 100 500 1000
        MarketType.PERP_COIN: 1000  # 5 10 20 50 100 500 1000
    }
    ORDERBOOK_UPDATE_INTERVAL: Dict[MarketType, str] = {
        MarketType.SPOT: "1000ms",     # 100ms 1000ms
        MarketType.PERP_USDT: "500ms", # 100ms 250ms 500ms
        MarketType.PERP_COIN: "500ms"  # 100ms 250ms 500ms
    }

class OkxConfig(BaseConfig):
    WS_URL: str = "wss://ws.okx.com:8443/ws/v5/business"
    # okx系统后台单个连接、交易产品维度，深度频道的推送顺序固定为：1档 -> 400档 -> 5档
    ORDERBOOK_DEPTH_LIMIT: int = 400 # 1 5 400
