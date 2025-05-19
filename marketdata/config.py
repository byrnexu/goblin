from typing import Dict, Any

class BaseConfig:
    """所有交易所通用的基础配置，可扩展"""
    pass

class BinanceConfig(BaseConfig):
    WS_URLS: Dict[str, str] = {
        'spot': "wss://stream.binance.com:9443/ws",
        'perp_usdt': "wss://fstream.binance.com/ws",
        'perp_coin': "wss://dstream.binance.com/ws"
    }
    REST_URLS: Dict[str, str] = {
        'spot': "https://api.binance.com/api/v3",
        'perp_usdt': "https://fapi.binance.com/fapi/v1",
        'perp_coin': "https://dapi.binance.com/dapi/v1"
    }
    ORDERBOOK_DEPTH_LIMIT: Dict[str, int] = {
        'spot': 5000, # 最大5000
        'perp_usdt': 1000, # 5 10 20 50 100 500 1000
        'perp_coin': 1000  # 5 10 20 50 100 500 1000
    }
    ORDERBOOK_UPDATE_INTERVAL: Dict[str, str] = {
        'spot': "1000ms",     # 100ms 1000ms
        'perp_usdt': "500ms", # 100ms 250ms 500ms
        'perp_coin': "500ms"  # 100ms 250ms 500ms
    }
    EVENT_TYPE_TRADE: Dict[str, str] = {
        'spot': "trade",
        'perp_usdt': "aggTrade",
        'perp_coin': "aggTrade"
    }

class OkxConfig(BaseConfig):
    WS_URL: str = "wss://ws.okx.com:8443/ws/v5/business"
    # okx系统后台单个连接、交易产品维度，深度频道的推送顺序固定为：1档 -> 400档 -> 5档
    ORDERBOOK_DEPTH_LIMIT: int = 400 # 1 5 400
