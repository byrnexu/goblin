"""
交易对符号转换工具

本模块用于在系统内部格式与各交易所格式之间进行交易对符号的转换。

- 系统格式：统一采用 BASE/QUOTE 或 BASE-QUOTE-PERP 形式（如 BTC/USDT, BTC-USDT-PERP, BTC-USD-PERP）
- 交易所格式：各交易所自有格式（如 BTCUSDT, BTCUSD_PERP, BTC-USD-SWAP 等）

支持的交易所及格式举例：
- binance_spot:    BTC/USDT <-> BTCUSDT
- binance_perp_usdt: BTC-USDT-PERP <-> BTCUSDT
- binance_perp_coin: BTC-USD-PERP <-> BTCUSD_PERP
- okx_spot:        BTC/USDT <-> BTC-USDT
- okx_perp_usdt:   BTC-USDT-PERP <-> BTC-USDT-SWAP
- okx_perp_coin:   BTC-USD-PERP <-> BTC-USD-SWAP

用法：
    to_exchange(symbol: str, exchange: str) -> str
    from_exchange(symbol: str, exchange: str) -> str

可通过自定义装饰器 @symbol_adapter 注册新交易所适配器。
"""
from typing import Callable, Dict
from functools import wraps

# 常见计价币种，按长度降序排列，避免如USDT/USDT等误判
_QUOTE_ASSETS = [
    "USDT", "BUSD", "USDC", "TUSD", "USDP", "DAI", "EUR", "BTC", "ETH", "BNB", "TRY", "RUB", "BRL", "AUD", "GBP", "UAH", "IDRT", "BIDR", "VAI", "NGN", "ZAR", "PLN", "RON", "UAH", "TRX", "XRP", "DOGE", "SHIB"
]

_symbol_adapters: Dict[str, Dict[str, Callable[[str], str]]] = {}

def symbol_adapter(exchange: str):
    """
    装饰器：用于简化交易所符号适配器的注册过程
    
    Args:
        exchange: 交易所名称，如 'binance_spot', 'okx_spot' 等
    Example:
        @symbol_adapter('binance_spot')
        def binance_spot_adapter(symbol: str) -> tuple[str, str]:
            # 返回 (to_exchange, from_exchange) 函数
            return (
                lambda s: s.replace("/", "").upper(),
                lambda s: _split_symbol_by_quote(s.upper())
            )
    """
    def decorator(func: Callable[[str], tuple[Callable[[str], str], Callable[[str], str]]]):
        @wraps(func)
        def wrapper(*args, **kwargs):
            to_exchange_func, from_exchange_func = func(*args, **kwargs)
            _symbol_adapters[exchange.lower()] = {
                "to_exchange": to_exchange_func,
                "from_exchange": from_exchange_func
            }
            return to_exchange_func, from_exchange_func
        # 立即执行函数以注册适配器
        wrapper(None)
        return wrapper
    return decorator

def _split_symbol_by_quote(symbol: str) -> str:
    """
    按常见计价币种结尾截取，将连续字符串分割为 base/quote 格式
    例：BTCUSDT -> BTC/USDT
    """
    for quote in _QUOTE_ASSETS:
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            return f"{base}/{quote}"
    return symbol

def to_exchange(symbol: str, exchange: str) -> str:
    """
    系统格式 -> 交易所格式
    
    Args:
        symbol: 系统内部格式，如 BTC/USDT, BTC-USDT-PERP
        exchange: 交易所适配器名，如 binance_spot
    Returns:
        交易所格式的symbol
    Raises:
        ValueError: 未找到适配器时抛出
    """
    try:
        adapter = _symbol_adapters[exchange.lower()]["to_exchange"]
        return adapter(symbol)
    except KeyError:
        raise ValueError(f"未找到交易所 {exchange} 的符号适配器")

def from_exchange(symbol: str, exchange: str) -> str:
    """
    交易所格式 -> 系统格式
    
    Args:
        symbol: 交易所格式，如 BTCUSDT, BTCUSD_PERP
        exchange: 交易所适配器名，如 binance_spot
    Returns:
        系统内部格式的symbol
    Raises:
        ValueError: 未找到适配器时抛出
    """
    try:
        adapter = _symbol_adapters[exchange.lower()]["from_exchange"]
        return adapter(symbol)
    except KeyError:
        raise ValueError(f"未找到交易所 {exchange} 的符号适配器")

# ======================== 适配器实现 ========================

@symbol_adapter('binance_spot')
def binance_spot_adapter(symbol: str) -> tuple[Callable[[str], str], Callable[[str], str]]:
    """
    Binance 现货
    系统格式:    BTC/USDT
    交易所格式:  BTCUSDT
    """
    return (
        lambda s: s.replace("/", "").upper(),
        lambda s: _split_symbol_by_quote(s.upper())
    )

@symbol_adapter('binance_perp_usdt')
def binance_perp_usdt_adapter(symbol: str) -> tuple[Callable[[str], str], Callable[[str], str]]:
    """
    Binance USDT本位永续合约
    系统格式:    BTC-USDT-PERP
    交易所格式:  BTCUSDT
    """
    return (
        lambda s: s[:-10] + "USDT" if s.endswith("-USDT-PERP") else s,
        lambda s: s[:-4] + "-USDT-PERP" if s.endswith("USDT") else s
    )

@symbol_adapter('binance_perp_coin')
def binance_perp_coin_adapter(symbol: str) -> tuple[Callable[[str], str], Callable[[str], str]]:
    """
    Binance 币本位永续合约
    系统格式:    BTC-USD-PERP
    交易所格式:  BTCUSD_PERP
    """
    return (
        lambda s: s[:-9] + "USD_PERP" if s.endswith("-USD-PERP") else s,
        lambda s: s[:-8] + "-USD-PERP" if s.endswith("USD_PERP") else s
    )

@symbol_adapter('okx_spot')
def okx_spot_adapter(symbol: str) -> tuple[Callable[[str], str], Callable[[str], str]]:
    """
    OKX 现货
    系统格式:    BTC/USDT
    交易所格式:  BTC-USDT
    """
    return (
        lambda s: s.replace("/", "-").upper(),
        lambda s: s.replace("-", "/").upper()
    )

@symbol_adapter('okx_perp_usdt')
def okx_perp_usdt_adapter(symbol: str) -> tuple[Callable[[str], str], Callable[[str], str]]:
    """
    OKX USDT本位永续合约
    系统格式:    BTC-USDT-PERP
    交易所格式:  BTC-USDT-SWAP
    """
    return (
        lambda s: s[:-4] + "SWAP" if s.endswith("-USDT-PERP") else s,
        lambda s: s[:-4] + "PERP" if s.endswith("-USDT-SWAP") else s
    )

@symbol_adapter('okx_perp_coin')
def okx_perp_coin_adapter(symbol: str) -> tuple[Callable[[str], str], Callable[[str], str]]:
    """
    OKX 币本位永续合约
    系统格式:    BTC-USD-PERP
    交易所格式:  BTC-USD-SWAP
    """
    return (
        lambda s: s[:-4] + "SWAP" if s.endswith("-USD-PERP") else s,
        lambda s: s[:-4] + "PERP" if s.endswith("-USD-SWAP") else s
    )

# 其他交易所可继续注册
