from enum import Enum, auto


class OrderType(Enum):

    MARKET = auto()


class TradeSide(Enum):

    BUY = auto()
    SELL = auto()
