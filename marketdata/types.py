"""
市场数据基础类型定义模块

本模块定义了市场数据相关的基础数据结构，包括：
1. OrderBookLevel: 订单簿中的单个价格档位
2. OrderBook: 完整的订单簿数据结构
3. Trade: 成交记录数据结构

这些类型被用于整个市场数据系统中，用于表示和处理交易所的实时数据。
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, Any
from sortedcontainers import SortedDict

@dataclass
class OrderBookLevel:
    """订单簿价格档位

    表示订单簿中的一个价格档位，包含价格和数量信息。
    每个价格档位代表在该价格上的所有订单的总量。

    Attributes:
        price: 价格，使用Decimal类型以保证精确计算
        quantity: 该价格档位的总数量，使用Decimal类型以保证精确计算
    """
    price: Decimal  # 价格
    quantity: Decimal  # 数量

@dataclass
class OrderBook:
    """订单簿数据结构

    包含交易对的完整订单簿信息，包括买单和卖单。
    订单簿是交易所中所有未成交订单的集合，按价格排序。

    Attributes:
        symbol: 交易对符号，例如 'BTCUSDT'
        bids: 买单集合，使用SortedDict按价格降序排列
        asks: 卖单集合，使用SortedDict按价格升序排列
        timestamp: 毫秒时间戳，表示订单簿更新的时间
        aux_data: 通用辅助数据字段，用于存储额外的信息，如lastUpdateId等

    Note:
        - bids和asks使用SortedDict而不是普通dict，以保证价格的有序性
        - bids按价格降序排列，asks按价格升序排列，便于快速获取最优价格
    """
    symbol: str  # 交易对符号，例如 'BTCUSDT'
    bids: SortedDict  # 价格降序
    asks: SortedDict  # 价格升序
    timestamp: int  # 毫秒时间戳，表示订单簿更新的时间
    aux_data: Dict[str, Any] = field(default_factory=dict)  # 通用辅助数据字段

@dataclass
class Trade:
    """成交数据结构

    表示一笔成交记录，记录了交易对中发生的每一笔交易。

    Attributes:
        symbol: 交易对符号，例如 'BTCUSDT'
        price: 成交价格，使用Decimal类型以保证精确计算
        quantity: 成交数量，使用Decimal类型以保证精确计算
        side: 成交方向，'buy'表示买方成交，'sell'表示卖方成交
        timestamp: 毫秒时间戳，表示成交发生的时间
        trade_id: 成交ID，用于唯一标识一笔成交，防止重复处理

    Note:
        - 成交方向是从成交发起方的角度来定义的
        - 买方成交意味着有人主动买入，卖方成交意味着有人主动卖出
    """
    symbol: str  # 交易对符号
    price: Decimal  # 成交价格
    quantity: Decimal  # 成交数量
    side: str  # 成交方向：'buy'表示买方成交，'sell'表示卖方成交
    timestamp: int  # 毫秒时间戳，表示成交时间
    trade_id: str  # 成交ID，用于唯一标识一笔成交 