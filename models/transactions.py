from datetime import datetime
from typing import Union

from dateutil.relativedelta import relativedelta
from pandas import Series

from models.attributes import TxType

NEWLINE = "\n"


class Transaction(object):

    def __init__(self, tx_id: int, tx_type: TxType, timestamp: Union[str, datetime], fiat_value: float,
                 fiat_tx_fee: float, currency_in: str, currency_in_volume: float, currency_in_fiat_price: float,
                 currency_out: str, currency_out_volume: float, currency_out_fiat_price: float, taxable: bool,
                 description: str = None):
        self.tx_id = tx_id
        self.tx_type = tx_type
        if isinstance(timestamp, str):
            self.timestamp = datetime.strptime(timestamp, "%m/%d/%Y")  # type: datetime
        else:
            self.timestamp = timestamp  # type: datetime
        self.fiat_value = fiat_value
        self.fiat_tx_fee = fiat_tx_fee
        self.currency_in = str.upper(currency_in)
        self.currency_in_volume = currency_in_volume
        self.currency_in_fiat_price = currency_in_fiat_price
        self.currency_out = str.upper(currency_out)
        self.currency_out_volume = currency_out_volume
        self.currency_out_fiat_price = currency_out_fiat_price
        self.taxable = taxable
        self.description = description

    def get_final_value(self) -> float:
        return self.fiat_value - self.fiat_tx_fee

    def is_taxable_event(self) -> bool:
        return False if self.tx_type == TxType.BUY.value else True

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"{', '.join([f'{k}={v}' for k, v in self.__dict__.items() if v is not None])}"
            f")"
        )

    # def __str__(self):
    #     return (
    #         f"{self.__class__.__name__}: {{{NEWLINE}  "
    #         f"{f',{NEWLINE}  '.join([f'{k}: {v}' for k, v in self.__dict__.items() if v is not None])}"
    #         f"{NEWLINE}}}"
    #     )


class Buy(Transaction):

    def __init__(self, tx_id: int, timestamp: Union[str, datetime], fiat_value: float, fiat_tx_fee: float,
                 currency_in: str, currency_in_volume: float, currency_in_fiat_price: float, currency_out: str,
                 currency_out_volume: float, currency_out_fiat_price: float, taxable: bool = False,
                 description: str = None):
        super().__init__(tx_id, TxType.BUY.value, timestamp, fiat_value, fiat_tx_fee, currency_in, currency_in_volume,
                         currency_in_fiat_price, currency_out, currency_out_volume, currency_out_fiat_price, taxable,
                         description)


class Sell(Transaction):

    def __init__(self, tx_id: int, timestamp: Union[str, datetime], fiat_value: float, fiat_tx_fee: float,
                 currency_in: str, currency_in_volume: float, currency_in_fiat_price: float, currency_out: str,
                 currency_out_volume: float, currency_out_fiat_price: float, taxable: bool = True,
                 description: str = None):
        super().__init__(tx_id, TxType.SELL.value, timestamp, fiat_value, fiat_tx_fee, currency_in, currency_in_volume,
                         currency_in_fiat_price, currency_out, currency_out_volume, currency_out_fiat_price, taxable,
                         description)


class Trade(Transaction):

    def __init__(self, tx_id: int, timestamp: Union[str, datetime], fiat_value: float, fiat_tx_fee: float,
                 currency_in: str, currency_in_volume: float, currency_in_fiat_price: float, currency_out: str,
                 currency_out_volume: float, currency_out_fiat_price: float, taxable: bool = True,
                 description: str = None):
        super().__init__(tx_id, TxType.TRADE.value, timestamp, fiat_value, fiat_tx_fee, currency_in, currency_in_volume,
                         currency_in_fiat_price, currency_out, currency_out_volume, currency_out_fiat_price, taxable,
                         description)


class Transact(Transaction):

    def __init__(self, tx_id: int, timestamp: Union[str, datetime], fiat_value: float, fiat_tx_fee: float,
                 currency_in: str, currency_in_volume: float, currency_in_fiat_price: float, currency_out: str,
                 currency_out_volume: float, currency_out_fiat_price: float, taxable: bool = True,
                 description: str = None):
        super().__init__(tx_id, TxType.TRANSACT.value, timestamp, fiat_value, fiat_tx_fee, currency_in,
                         currency_in_volume, currency_in_fiat_price, currency_out, currency_out_volume,
                         currency_out_fiat_price, taxable, description)


class TaxableTransaction(object):

    def __init__(self, cryptocurrency: str, tx_in: Transaction, tx_out: Union[Transaction, None]):
        self.cryptocurrency = cryptocurrency
        self.tx_in = tx_in
        self.tx_out = tx_out
        self.date_acquired = self.tx_in.timestamp if self.tx_in else None
        self.date_acquired_str = self.tx_in.timestamp.strftime("%m/%d/%Y") if self.tx_in else None
        self.date_sold = self.tx_out.timestamp if self.tx_out else None
        self.date_sold_str = self.tx_out.timestamp.strftime("%m/%d/%Y") if self.tx_out else None
        self.sales_proceeds = round(self.tx_out.fiat_value - self.tx_out.fiat_tx_fee) if self.tx_out else None
        self.cost_basis = round(self.tx_in.fiat_value - self.tx_in.fiat_tx_fee) if self.tx_in else None
        self.short_term = (
            relativedelta(self.date_sold, self.date_acquired).years == 0
            if self.date_sold and self.date_acquired_str
            else None
        )
        self.long_term = not self.short_term if self.short_term else None
        self.capital_gain_or_loss = (
            round(self.sales_proceeds - self.cost_basis) if self.sales_proceeds and self.cost_basis else None
        )
        self.lot_description = (
            f"{round(self.tx_in.currency_out_volume, 2)} {cryptocurrency.upper()} - CRYPTO"
            if self.tx_in
            else f"{round(self.tx_out.currency_out_volume, 2)} {cryptocurrency.upper()} - CRYPTO"
        )

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join([f'{k}={v}' for k, v in self.__dict__.items()])})"

    def __str__(self):
        return (
            f"{self.__class__.__name__}: {{{NEWLINE}  "
            f"{f',{NEWLINE}  '.join([f'{k}: {v}' for k, v in self.__dict__.items()])}"
            f"{NEWLINE}}}"
        )

    def to_pd_series(self):
        exclude_fields = ["tx_in", "tx_out"]
        series_data = {k: v for k, v in self.__dict__.items() if k not in exclude_fields}
        return Series(data=series_data)
