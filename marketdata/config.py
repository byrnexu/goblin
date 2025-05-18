class BaseConfig:
    """所有交易所通用的基础配置，可扩展"""
    pass

class BinanceSpotConfig(BaseConfig):
    WS_URL = "wss://stream.binance.com:9443/ws"
    REST_URL = "https://api.binance.com/api/v3"
    ORDERBOOK_DEPTH_LIMIT = 5000 # 最大5000
    ORDERBOOK_UPDATE_INTERVAL = "1000ms" # 100ms 1000ms

class BinancePerpConfig(BaseConfig):
    WS_URLS = {
        'usdt': "wss://fstream.binance.com/ws",
        'coin': "wss://dstream.binance.com/ws"
    }
    REST_URLS = {
        'usdt': "https://fapi.binance.com/fapi/v1",
        'coin': "https://dapi.binance.com/dapi/v1"
    }
    ORDERBOOK_DEPTH_LIMIT = 1000 # 5 10 20 50 100 500 1000
    ORDERBOOK_UPDATE_INTERVAL = "500ms" # 100ms 250ms 500ms

class OkxConfig(BaseConfig):
    WS_URL = "wss://ws.okx.com:8443/ws/v5/business"
    # okx系统后台单个连接、交易产品维度，深度频道的推送顺序固定为：1档 -> 400档 -> 5档
    ORDERBOOK_DEPTH_LIMIT = 400 # 1 5 400
