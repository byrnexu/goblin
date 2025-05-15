from typing import Callable, Dict

# 常见计价币种，按长度降序排列，避免如USDT/USDT等误判
_QUOTE_ASSETS = [
    "USDT", "BUSD", "USDC", "TUSD", "USDP", "DAI", "EUR", "BTC", "ETH", "BNB", "TRY", "RUB", "BRL", "AUD", "GBP", "UAH", "IDRT", "BIDR", "VAI", "NGN", "ZAR", "PLN", "RON", "UAH", "TRX", "XRP", "DOGE", "SHIB"
]

_symbol_adapters: Dict[str, Dict[str, Callable[[str], str]]] = {}

def register_symbol_adapter(exchange: str, to_exchange: Callable[[str], str], from_exchange: Callable[[str], str]):
    _symbol_adapters[exchange.lower()] = {
        "to_exchange": to_exchange,
        "from_exchange": from_exchange
    }

def to_exchange(symbol: str, exchange: str) -> str:
    """系统格式 -> 交易所格式"""
    adapter = _symbol_adapters[exchange.lower()]["to_exchange"]
    return adapter(symbol)

def from_exchange(symbol: str, exchange: str) -> str:
    """交易所格式 -> 系统格式"""
    adapter = _symbol_adapters[exchange.lower()]["from_exchange"]
    return adapter(symbol)

# BinanceSpot适配器

def _binance_to_exchange(symbol: str) -> str:
    # BTC/USDT -> BTCUSDT
    return symbol.replace("/", "").upper()

def _binance_from_exchange(symbol: str) -> str:
    # BTCUSDT -> BTC/USDT，按常见计价币种结尾截取
    symbol = symbol.upper()
    for quote in _QUOTE_ASSETS:
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            return f"{base}/{quote}"
    # fallback: 不识别则原样返回
    return symbol

register_symbol_adapter("binance_spot", _binance_to_exchange, _binance_from_exchange)

# 其他交易所可继续注册
