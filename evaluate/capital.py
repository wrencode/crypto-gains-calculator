from collections import defaultdict
from copy import deepcopy
from enum import Enum
from typing import List, Union

from pandas import concat, to_datetime

from data.extract import TxData
from models.transactions import Transaction, Buy, Sell, Transact, TaxableTransaction

RD = 8


class TxFlow(Enum):
    IN = "in"
    OUT = "out"


# noinspection DuplicatedCode
def split_unequal_tx(smaller_volume_tx: Transaction, greater_volume_tx: Transaction, greater_volume_tx_flow: TxFlow):
    opposite_flow = None  # type: Union[None, TxFlow]
    for flow in TxFlow:
        if flow.value != greater_volume_tx_flow.value:
            opposite_flow = flow

    greater_volume_tx_processed_pct = (
            getattr(smaller_volume_tx, f"currency_{greater_volume_tx_flow.value}_volume")
            / getattr(greater_volume_tx, f"currency_{opposite_flow.value}_volume")
    )

    greater_volume_tx_processed_split = deepcopy(greater_volume_tx)
    greater_volume_tx_processed_split.currency_in_volume = (
        round(float(greater_volume_tx_processed_split.currency_in_volume * greater_volume_tx_processed_pct), RD)
    )
    greater_volume_tx_processed_split.currency_out_volume = (
        round(float(greater_volume_tx_processed_split.currency_out_volume * greater_volume_tx_processed_pct), RD)
    )
    greater_volume_tx_processed_split.fiat_value = (
        round(float(greater_volume_tx_processed_split.fiat_value * greater_volume_tx_processed_pct), RD)
    )
    greater_volume_tx_processed_split.fiat_tx_fee = (
        round(float(greater_volume_tx_processed_split.fiat_tx_fee * greater_volume_tx_processed_pct), RD)
    )

    greater_volume_tx_unprocessed_pct = 1 - greater_volume_tx_processed_pct

    greater_volume_tx_unprocessed_split = deepcopy(greater_volume_tx)
    greater_volume_tx_unprocessed_split.currency_in_volume = (
        round(float(greater_volume_tx_unprocessed_split.currency_in_volume * greater_volume_tx_unprocessed_pct), RD)
    )
    greater_volume_tx_unprocessed_split.currency_out_volume = (
        round(float(greater_volume_tx_unprocessed_split.currency_out_volume * greater_volume_tx_unprocessed_pct), RD)
    )
    greater_volume_tx_unprocessed_split.fiat_value = (
        round(float(greater_volume_tx_unprocessed_split.fiat_value * greater_volume_tx_unprocessed_pct), RD)
    )
    greater_volume_tx_unprocessed_split.fiat_tx_fee = (
        round(float(greater_volume_tx_unprocessed_split.fiat_tx_fee * greater_volume_tx_unprocessed_pct), RD)
    )

    return greater_volume_tx_processed_split, greater_volume_tx_unprocessed_split


class TaxableCrypto(object):

    def __init__(self, tax_year: int = None, fiat_currency: str = "usd", sort_field: str = "timestamp",
                 sort_direction: str = "ascending", expenditure_types: List[str] = None):
        self.tax_year = tax_year
        self.fiat_currency = fiat_currency
        self.sort_field = sort_field
        self.sort_direction = sort_direction
        self.expenditure_types = expenditure_types if expenditure_types else ["purchase", "donation", "gift"]
        self.capital_gains_and_losses = []
        self.taxable_income = defaultdict(list)

    @staticmethod
    def _get_df_from_tx_list(taxable_txs: List[TaxableTransaction], exclude_columns: List[str] = None):

        taxable_txs_df = concat(
            [taxable_tx.to_pd_series() for taxable_tx in taxable_txs],
            axis=1
        ).T

        taxable_txs_df["cryptocurrency"] = taxable_txs_df["cryptocurrency"].str.upper()
        taxable_txs_df.index += 1
        taxable_txs_df.index.name = "tx_count"

        column_order = [
            "cryptocurrency",
            "lot_description",
            "date_acquired",
            "date_acquired_str",
            "date_sold",
            "date_sold_str",
            "sales_proceeds",
            "cost_basis",
            "capital_gain_or_loss",
            "short_term",
            "long_term"
        ]
        column_order = [col for col in column_order if col not in exclude_columns]

        taxable_txs_df = taxable_txs_df[column_order]

        return taxable_txs_df

    # noinspection DuplicatedCode
    def get_capital_gains_and_losses(self):

        tx_data = TxData(self.fiat_currency)
        buys = tx_data.retrieve_buy_events(self.tax_year, self.fiat_currency, self.sort_field, self.sort_direction)
        sells = tx_data.retrieve_sell_events(self.tax_year, self.fiat_currency, self.sort_field, self.sort_direction)
        transacts = tx_data.retrieve_transact_events(self.tax_year, self.sort_field, self.sort_direction)

        crypto_tallies = {
            ticker: {
                "in": [],
                "out": []
            } for ticker in tx_data.get_cryptocurrencies() if ticker.lower() not in self.expenditure_types
        }

        for tx in buys:  # type: Buy
            ticker = tx.currency_out.lower()
            crypto_tallies[ticker]["in"].append(tx)

        for tx in sells:  # type: Sell
            ticker = tx.currency_in.lower()
            crypto_tallies[ticker]["out"].append(tx)

        for tx in transacts:  # type: Transact
            ticker_to = tx.currency_out.lower()
            ticker_from = tx.currency_in.lower()
            if ticker_to in crypto_tallies.keys():
                crypto_tallies[ticker_to]["in"].append(tx)
                self.taxable_income[ticker_to].append(tx)
            elif ticker_from in crypto_tallies.keys():
                crypto_tallies[ticker_from]["out"].append(tx)

        for tallies in crypto_tallies.values():
            tallies["in"] = sorted(tallies["in"], key=lambda x: x.timestamp)
            tallies["out"] = sorted(tallies["out"], key=lambda x: x.timestamp)

        for ticker, tallies in crypto_tallies.items():

            # if ticker != "yldy":
            #     continue
            #
            # print(f"Transactions for {ticker}:")
            # print()

            txs_in = tallies["in"]  # type: List[Transaction]
            txs_out = tallies["out"]  # type: List[Transaction]

            if len(txs_out) == 0:
                continue

            split_txs_in = []
            split_txs_out = []

            running_total_tx_in_volume = 0
            running_total_tx_out_volume = 0
            running_total_ticker_volume = 0
            while len(txs_out) > 0:

                first_tx_in = txs_in.pop(0)
                first_tx_out = txs_out.pop(0)

                if first_tx_out.timestamp < first_tx_in.timestamp:
                    raise IndexError("Currency out transaction detected before currency in transaction, check data!")

                if first_tx_in.currency_out_volume == first_tx_out.currency_in_volume:

                    split_txs_in.append(first_tx_in)
                    split_txs_out.append(first_tx_out)

                    running_total_tx_in_volume += first_tx_in.currency_out_volume
                    running_total_tx_out_volume += first_tx_out.currency_in_volume
                    running_total_ticker_volume += (first_tx_in.currency_out_volume - first_tx_out.currency_in_volume)

                    if self.tax_year:
                        if first_tx_out.timestamp.year == self.tax_year:
                            self.capital_gains_and_losses.append(TaxableTransaction(ticker, first_tx_in, first_tx_out))
                    else:
                        self.capital_gains_and_losses.append(TaxableTransaction(ticker, first_tx_in, first_tx_out))

                elif first_tx_in.currency_out_volume > first_tx_out.currency_in_volume:

                    first_tx_in_sold_split, first_tx_in_unsold_split = split_unequal_tx(
                        smaller_volume_tx=first_tx_out, greater_volume_tx=first_tx_in, greater_volume_tx_flow=TxFlow.IN
                    )

                    split_txs_in.append(first_tx_in_sold_split)
                    split_txs_out.append(first_tx_out)

                    running_total_tx_in_volume += first_tx_in_sold_split.currency_out_volume
                    running_total_tx_out_volume += first_tx_out.currency_in_volume
                    running_total_ticker_volume += (
                            first_tx_in_sold_split.currency_out_volume - first_tx_out.currency_in_volume
                    )

                    txs_in.insert(0, first_tx_in_unsold_split)

                    if self.tax_year:
                        if first_tx_out.timestamp.year == self.tax_year:
                            self.capital_gains_and_losses.append(
                                TaxableTransaction(ticker, first_tx_in_sold_split, first_tx_out)
                            )
                    else:
                        self.capital_gains_and_losses.append(
                            TaxableTransaction(ticker, first_tx_in_sold_split, first_tx_out)
                        )

                elif first_tx_in.currency_out_volume < first_tx_out.currency_in_volume:

                    first_tx_out_bought_split, first_tx_out_unbought_split = split_unequal_tx(
                        smaller_volume_tx=first_tx_in, greater_volume_tx=first_tx_out, greater_volume_tx_flow=TxFlow.OUT
                    )

                    split_txs_in.append(first_tx_in)
                    split_txs_out.append(first_tx_out_bought_split)

                    running_total_tx_in_volume += first_tx_in.currency_out_volume
                    running_total_tx_out_volume += first_tx_out_bought_split.currency_in_volume
                    running_total_ticker_volume += (
                            first_tx_in.currency_out_volume - first_tx_out_bought_split.currency_in_volume
                    )

                    txs_out.insert(0, first_tx_out_unbought_split)

                    if self.tax_year:
                        if first_tx_out_bought_split.timestamp.year == self.tax_year:
                            self.capital_gains_and_losses.append(
                                TaxableTransaction(ticker, first_tx_in, first_tx_out_bought_split)
                            )
                    else:
                        self.capital_gains_and_losses.append(
                            TaxableTransaction(ticker, first_tx_in, first_tx_out_bought_split)
                        )

        return sorted(
            [taxable_tx for taxable_tx in self.capital_gains_and_losses if taxable_tx.capital_gain_or_loss != 0],
            key=lambda x: (x.short_term, x.date_sold)
        )

    def get_taxable_income(self):

        taxable_income_txs = []
        for ticker, txs in self.taxable_income.items():
            for tx in txs:  # type: Transaction
                taxable_income_txs.append(TaxableTransaction(ticker, tx, None))

        return sorted(
            [taxable_tx for taxable_tx in taxable_income_txs if taxable_tx.cost_basis != 0],
            key=lambda x: x.date_acquired
        )

    def get_capital_gains_and_losses_df(self, exclude_columns: List[str] = None):
        return self._get_df_from_tx_list(self.get_capital_gains_and_losses(), exclude_columns)

    def get_taxable_income_df(self, exclude_columns: List[str] = None):
        exclude_columns.extend([
            "date_sold_str",
            "sales_proceeds",
            "capital_gain_or_loss",
            "short_term",
            "long_term"
        ])
        taxable_income_df = self._get_df_from_tx_list(self.get_taxable_income(), exclude_columns)
        return taxable_income_df.loc[
            (to_datetime(
                taxable_income_df["date_acquired_str"], format="%m/%d/%Y") >= f"{self.tax_year}-01-01 00:00:00")
            &
            (to_datetime(
                taxable_income_df["date_acquired_str"], format="%m/%d/%Y") < f"{self.tax_year + 1}-01-01 00:00:00")
            ]
