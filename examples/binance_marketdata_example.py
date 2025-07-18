import asyncio
from decimal import Decimal
import sys
import os
import signal

# 添加项目根目录到Python路径，确保可以导入marketdata模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from marketdata.binance import BinanceMarketData
from marketdata.base import OrderBook, Trade
from marketdata.config import BinanceConfig
from marketdata.types import MarketType

async def orderbook_callback(orderbook: OrderBook) -> None:
    """订单簿数据回调函数

    当收到订单簿更新时，打印最新的订单簿状态

    Args:
        orderbook: 更新后的订单簿数据
    """
    print(f"\n订单簿更新 - {orderbook.symbol}")
    print("买单:")
    for bid in list(orderbook.bids.values())[:5]:  # 只显示前5档
        print(f"价格: {bid.price:>10.4f}, 数量: {bid.quantity:>10.4f}")
    print("卖单:")
    for ask in list(orderbook.asks.values())[:5]:  # 只显示前5档
        print(f"价格: {ask.price:>10.4f}, 数量: {ask.quantity:>10.4f}")

async def trade_callback(trade: Trade) -> None:
    """成交数据回调函数

    当收到新的成交时，打印成交信息

    Args:
        trade: 新的成交数据
    """
    print(f"\n成交 - {trade.symbol}")
    print(f"价格: {trade.price:>10.2f}")
    print(f"数量: {trade.quantity:>10.4f}")
    print(f"方向: {'买入' if trade.side == 'buy' else '卖出'}")
    print(f"成交ID: {trade.trade_id}")

async def main():
    """主函数

    演示如何使用市场数据订阅系统：
    1. 创建市场数据实例
    2. 连接到交易所
    3. 订阅数据
    4. 处理程序退出
    """
    # 创建配置对象
    config = BinanceConfig()

    # 创建币安现货市场数据实例
    binance_spot_market_data = BinanceMarketData(config, market_type=MarketType.SPOT)
    await binance_spot_market_data.connect()
    binance_spot_market_data.subscribe_orderbook("JUP/USDT", orderbook_callback)
    binance_spot_market_data.subscribe_orderbook("SOL/USDT", orderbook_callback)
    binance_spot_market_data.subscribe_trades("BTC/USDT", trade_callback)

    # 创建币安USDT本位永续合约市场数据实例
    binance_perp_usdt_market_data = BinanceMarketData(config, market_type=MarketType.PERP_USDT)
    await binance_perp_usdt_market_data.connect()
    binance_perp_usdt_market_data.subscribe_orderbook("BTC-USDT-PERP", orderbook_callback)
    binance_perp_usdt_market_data.subscribe_trades("BTC-USDT-PERP", trade_callback)

    # 创建币安币本位永续合约市场数据实例
    binance_perp_coin_market_data = BinanceMarketData(config, market_type=MarketType.PERP_COIN)
    await binance_perp_coin_market_data.connect()
    binance_perp_coin_market_data.subscribe_orderbook("BTC-USD-PERP", orderbook_callback)
    binance_perp_coin_market_data.subscribe_trades("BTC-USD-PERP", trade_callback)

    # 创建事件来通知程序退出
    stop_event = asyncio.Event()

    def signal_handler():
        """处理Ctrl+C信号

        当用户按下Ctrl+C时，设置停止事件
        """
        print("\n正在关闭程序...")
        stop_event.set()

    # 注册信号处理器
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, signal_handler)
    loop.add_signal_handler(signal.SIGTERM, signal_handler)

    try:
        # 等待停止信号
        await stop_event.wait()
    finally:
        # 断开连接
        await binance_spot_market_data.disconnect()
        await binance_perp_usdt_market_data.disconnect()
        await binance_perp_coin_market_data.disconnect()
        print("程序已关闭")

if __name__ == "__main__":
    try:
        # 运行主函数
        asyncio.run(main())
    except KeyboardInterrupt:
        # 忽略KeyboardInterrupt异常，因为我们已经处理了SIGINT信号
        pass
