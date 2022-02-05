from abc import ABC, abstractmethod
from collections import OrderedDict
from itertools import chain
from typing import Union, List, Type

from pandas import DataFrame, to_datetime

from models.attributes import TxType
from models.transactions import Buy, Sell, Trade, Transact

RD = 8


class BaseTable(ABC):

    @abstractmethod
    def __init__(self, fiat_currency: str):
        self.data = None  # type: Union[DataFrame, None]
        self.data_col_index_map = OrderedDict()
        self.fiat_currency = fiat_currency.upper()
        self.cryptocurrencies = set()
        self._tx_count = 1

    def _create_tx_class_instance_list(self, class_type: Type,
                                       df: DataFrame) -> List[Union[Buy, Sell, Trade, Transact]]:

        txs = []
        for row in df.itertuples():
            txs.append(class_type(
                tx_id=self._tx_count,
                timestamp=row[self.data_col_index_map["tx_timestamp"]],
                fiat_value=round(float(row[self.data_col_index_map["fiat_value"]]), RD),
                fiat_tx_fee=round(float(row[self.data_col_index_map["fiat_tx_fee"]]), RD),
                currency_in=row[self.data_col_index_map["currency_in"]],
                currency_in_fiat_price=round(float(row[self.data_col_index_map["currency_in_fiat_price"]]), RD),
                currency_in_volume=round(float(row[self.data_col_index_map["currency_in_volume"]]), RD),
                currency_out=row[self.data_col_index_map["currency_out"]],
                currency_out_fiat_price=round(float(row[self.data_col_index_map["currency_out_fiat_price"]]), RD),
                currency_out_volume=round(float(row[self.data_col_index_map["currency_out_volume"]]), RD),
                taxable=row[self.data_col_index_map["tx_taxable"]],
                description=row[self.data_col_index_map["description"]]
            ))
            ticker = str.lower(row[self.data_col_index_map["currency_out"]])
            if ticker != self.fiat_currency.lower():
                self.cryptocurrencies.add(str.lower(row[self.data_col_index_map["currency_out"]]))
            self._tx_count += 1
        return txs

    def _get_transactions(self, tx_type: str, tx_type_class: Type, tax_year: int, sort_field: str,
                          sort_direction: str) -> List[Union[Buy, Sell, Trade, Transact]]:

        if tax_year:
            df = self.data[
                (self.data["tx_type"].str.lower() == tx_type)
                # & (to_datetime(self.data["tx_timestamp"], format="%m/%d/%Y") > f"{tax_year}-01-01 00:00:00")
                & (to_datetime(self.data["tx_timestamp"], format="%m/%d/%Y") < f"{tax_year + 1}-01-01 00:00:00")
                ]
        else:
            df = self.data[self.data["tx_type"].str.lower() == tx_type]

        return (
            sorted(
                self._create_tx_class_instance_list(tx_type_class, df),
                key=lambda x: getattr(x, sort_field),
                reverse=True if sort_direction == "descending" else False
            )
            if sort_field
            else self._create_tx_class_instance_list(tx_type_class, df)
        )

    def get_buys(self, tax_year: int, sort_field: str, sort_direction: str) -> List[Buy]:
        return self._get_transactions(
            TxType.BUY.value, Buy, tax_year=tax_year, sort_field=sort_field, sort_direction=sort_direction
        )

    def get_sells(self, tax_year: int, sort_field: str, sort_direction: str) -> List[Sell]:
        return self._get_transactions(
            TxType.SELL.value, Sell, tax_year=tax_year, sort_field=sort_field, sort_direction=sort_direction
        )

    def get_trades(self, tax_year: int, sort_field: str, sort_direction: str) -> List[Trade]:
        return self._get_transactions(
            TxType.TRADE.value, Trade, tax_year=tax_year, sort_field=sort_field, sort_direction=sort_direction
        )

    def get_transacts(self, tax_year: int, sort_field: str, sort_direction: str) -> List[Transact]:
        return self._get_transactions(
            TxType.TRANSACT.value, Transact, tax_year=tax_year, sort_field=sort_field, sort_direction=sort_direction
        )

    def get_buys_from_trades(self, tax_year: int, fiat_currency: str, sort_field: str,
                             sort_direction: str) -> List[Buy]:

        trades = self.get_trades(tax_year, sort_field, sort_direction)

        buy_components_of_trades = []
        for trade in trades:  # type: Trade
            buy_components_of_trades.append(
                Buy(
                    tx_id=trade.tx_id,
                    timestamp=trade.timestamp,
                    fiat_value=trade.fiat_value,
                    fiat_tx_fee=trade.fiat_tx_fee,
                    currency_in=fiat_currency,
                    currency_in_volume=trade.fiat_value,
                    currency_in_fiat_price=1,
                    currency_out=trade.currency_out,
                    currency_out_volume=trade.currency_out_volume,
                    currency_out_fiat_price=trade.currency_out_fiat_price,
                    taxable=False,
                    description=f"Buy component of trade: {trade}"
                )
            )
        return buy_components_of_trades

    def get_sells_from_trades(self, tax_year: int, fiat_currency: str, sort_field: str,
                              sort_direction: str) -> List[Sell]:

        trades = self.get_trades(tax_year, sort_field, sort_direction)

        sell_components_of_trades = []
        for trade in trades:  # type: Trade
            sell_components_of_trades.append(
                Sell(
                    tx_id=trade.tx_id,
                    timestamp=trade.timestamp,
                    fiat_value=trade.fiat_value,
                    fiat_tx_fee=trade.fiat_tx_fee,
                    currency_in=trade.currency_in,
                    currency_in_volume=trade.currency_in_volume,
                    currency_in_fiat_price=trade.currency_in_fiat_price,
                    currency_out=fiat_currency,
                    currency_out_volume=trade.fiat_value,
                    currency_out_fiat_price=1,
                    taxable=True,
                    description=f"Sell component of trade: {trade}"
                )
            )
        return sell_components_of_trades

    def get_buy_events(self, tax_year: int, fiat_currency: str, sort_field: str, sort_direction: str) -> List[Buy]:
        return (
            sorted(
                list(chain(
                    self.get_buys(tax_year, sort_field, sort_direction),
                    self.get_buys_from_trades(tax_year, fiat_currency, sort_field, sort_direction)
                )),
                key=lambda x: getattr(x, sort_field),
                reverse=True if sort_direction == "descending" else False
            )
            if sort_field
            else list(chain(
                self.get_buys(tax_year, sort_field, sort_direction),
                self.get_buys_from_trades(tax_year, fiat_currency, sort_field, sort_direction)
            ))
        )

    def get_sell_events(self, tax_year: int, fiat_currency: str, sort_field: str,
                        sort_direction: str) -> List[Union[Sell, Transact]]:
        return (
            sorted(
                list(chain(
                    self.get_sells(tax_year, sort_field, sort_direction),
                    self.get_sells_from_trades(tax_year, fiat_currency, sort_field, sort_direction)
                )),
                key=lambda x: getattr(x, sort_field),
                reverse=True if sort_direction == "descending" else False
            )
            if sort_field
            else list(chain(
                self.get_sells(tax_year, sort_field, sort_direction),
                self.get_sells_from_trades(tax_year, fiat_currency, sort_field, sort_direction)
            ))
        )
