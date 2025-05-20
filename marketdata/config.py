from typing import Dict, Any
from .types import MarketType, Market

class BaseConfig:
    """所有交易所通用的基础配置，可扩展"""
    pass

class BinanceConfig(BaseConfig):
    MARKET: Market = Market.BINANCE
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
    MARKET: Market = Market.OKX
    WS_URLS: Dict[MarketType, str] = {
        MarketType.SPOT: "wss://ws.okx.com:8443/ws/v5/public",
        MarketType.PERP_USDT: "wss://ws.okx.com:8443/ws/v5/public",
        MarketType.PERP_COIN: "wss://ws.okx.com:8443/ws/v5/public"
    }
    # OKX后台逻辑：
    # 1档：首次推1档快照数据，以后定量推送，每10毫秒当1档快照数据有变化推送一次1档数据
    # 5档：首次推5档快照数据，以后定量推送，每100毫秒当5档快照数据有变化推送一次5档数据
    # 400档：首次推400档快照数据，以后增量推送，每10毫秒推送一次变化的数据
    # okx系统后台单个连接、交易产品维度，深度频道的推送顺序固定为：1档 -> 400档 -> 5档
    ORDERBOOK_DEPTH_LIMIT: Dict[MarketType, int] = {
        MarketType.SPOT: 1, # 1 5 400
        MarketType.PERP_USDT: 5, # 1 5 400
        MarketType.PERP_COIN: 400  # 1 5 400
    }
