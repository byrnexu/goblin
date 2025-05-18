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

# BinancePerp适配器（USDT本位和币本位）

def _binance_perp_usdt_to_exchange(symbol: str) -> str:
    # BTC-USDT-PERP -> BTCUSDT
    if symbol.endswith("-USDT-PERP"):
        base = symbol[:-10]
        return f"{base}USDT"
    return symbol

def _binance_perp_usdt_from_exchange(symbol: str) -> str:
    # BTCUSDT -> BTC-USDT-PERP
    symbol = symbol.upper()
    if symbol.endswith("USDT"):
        base = symbol[:-4]
        return f"{base}-USDT-PERP"
    return symbol

register_symbol_adapter("binance_perp_usdt", _binance_perp_usdt_to_exchange, _binance_perp_usdt_from_exchange)

def _binance_perp_coin_to_exchange(symbol: str) -> str:
    # BTC-USD-PERP -> BTCUSD_PERP
    if symbol.endswith("-USD-PERP"):
        base = symbol[:-9]
        return f"{base}USD_PERP"
    return symbol

def _binance_perp_coin_from_exchange(symbol: str) -> str:
    # BTCUSD_PERP -> BTC-USD-PERP
    symbol = symbol.upper()
    if symbol.endswith("USD_PERP"):
        base = symbol[:-8]
        return f"{base}-USD-PERP"
    return symbol

register_symbol_adapter("binance_perp_coin", _binance_perp_coin_to_exchange, _binance_perp_coin_from_exchange)

# OKX适配器

def _okx_spot_to_exchange(symbol: str) -> str:
    return symbol

def _okx_spot_from_exchange(symbol: str) -> str:
    return symbol

register_symbol_adapter("okx_spot", _okx_spot_to_exchange, _okx_spot_from_exchange)

def _okx_perp_usdt_to_exchange(symbol: str) -> str:
    # BTC-USDT-PERP -> BTC-USDT_PERP
    if symbol.endswith("-USDT-PERP"):
        return symbol[:-5] + "_" + symbol[-4:]
    return symbol

def _okx_perp_usdt_from_exchange(symbol: str) -> str:
    # BTC-USDT_PERP -> BTC-USDT-PERP
    if symbol.endswith("-USDT_PERP"):
        return symbol[:-5] + "-" + symbol[-4:]
    return symbol

register_symbol_adapter("okx_perp_usdt", _okx_perp_usdt_to_exchange, _okx_perp_usdt_from_exchange)

def _okx_perp_coin_to_exchange(symbol: str) -> str:
    # BTC-USD-PERP -> BTCUSD_PERP
    if symbol.endswith("-USD-PERP"):
        return symbol.replace("-USD-PERP", "USD_PERP")
    return symbol

def _okx_perp_coin_from_exchange(symbol: str) -> str:
    # BTCUSD_PERP -> BTC-USD-PERP
    if symbol.endswith("USD_PERP"):
        return symbol.replace("USD_PERP", "-USD-PERP")
    return symbol

register_symbol_adapter("okx_perp_coin", _okx_perp_coin_to_exchange, _okx_perp_coin_from_exchange)

# 其他交易所可继续注册
