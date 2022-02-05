from enum import Enum


class TxType(Enum):
    BUY = "buy"
    SELL = "sell"
    TRADE = "trade"
    TRANSACT = "transact"
