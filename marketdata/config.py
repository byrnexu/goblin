class BaseConfig:
    """所有交易所通用的基础配置，可扩展"""
    pass

class BinanceConfig(BaseConfig):
    WS_URL = "wss://stream.binance.com:9443/ws"
    REST_URL = "https://api.binance.com/api/v3"
    ORDERBOOK_DEPTH_LIMIT = 5000
    ORDERBOOK_UPDATE_INTERVAL = "1000ms"
