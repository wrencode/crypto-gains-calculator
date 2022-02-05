import os
from importlib import import_module
from itertools import chain
from pathlib import Path
from typing import Set

from dotenv import load_dotenv

from data.dao.table import BaseTable

load_dotenv(Path(__file__).parent.parent / ".env")

DATA_SOURCE = os.getenv("DATA_SOURCE")


# noinspection PyPep8Naming
class TxData(object):

    def __init__(self, fiat_currency: str):
        DataTable = getattr(import_module(f"data.dao.{DATA_SOURCE.lower()}"), f"{DATA_SOURCE.capitalize()}Table")
        self.data = DataTable(
            fiat_currency,
            "10Fco8GhmN1LbGb9RfDCGTosEsZGGp3Yb9mkOW39al0k",
            "transactions",
            "A",
            "Y"
        )  # type: BaseTable

    def get_cryptocurrencies(self) -> Set[str]:
        return self.data.cryptocurrencies

    def retrieve_buy_events(self, tax_year: int, fiat_currency: str, sort_field: str, sort_direction: str):
        return self.data.get_buy_events(
            tax_year=tax_year, fiat_currency=fiat_currency, sort_field=sort_field, sort_direction=sort_direction
        )

    def retrieve_sell_events(self, tax_year: int, fiat_currency: str, sort_field: str, sort_direction: str):
        return self.data.get_sell_events(
            tax_year=tax_year, fiat_currency=fiat_currency, sort_field=sort_field, sort_direction=sort_direction
        )

    def retrieve_transact_events(self, tax_year: int, sort_field: str, sort_direction: str):
        return self.data.get_transacts(tax_year=tax_year, sort_field=sort_field, sort_direction=sort_direction)

    def retrieve_nontaxable_events(self, tax_year: int, fiat_currency: str, sort_field: str, sort_direction: str):
        return self.data.get_buy_events(
            tax_year=tax_year, fiat_currency=fiat_currency, sort_field=sort_field, sort_direction=sort_direction
        )

    def retrieve_taxable_events(self, tax_year: int, fiat_currency: str, sort_field: str, sort_direction: str):
        return list(chain(
            self.data.get_sell_events(
                tax_year=tax_year, fiat_currency=fiat_currency, sort_field=sort_field, sort_direction=sort_direction
            ),
            self.data.get_transacts(tax_year=tax_year, sort_field=sort_field, sort_direction=sort_direction)
        ))
