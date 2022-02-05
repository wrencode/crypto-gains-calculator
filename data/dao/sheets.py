import json
import os
from collections import OrderedDict
from decimal import Decimal
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pandas import options, to_datetime, DataFrame

from data.dao.table import BaseTable

RD = 8


class SheetsTable(BaseTable):

    def __init__(self, fiat_currency: str, sheet_id: str, tab_name: str, starting_range_col: str,
                 ending_range_col: str, starting_range_row: str = "", ending_range_row: str = ""):
        super().__init__(fiat_currency=fiat_currency)

        # If modifying these scopes, delete the file token.json
        # scopes can be found here: https://developers.google.com/identity/protocols/oauth2/scopes
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

        google_auth_dir = Path(__file__).parent.parent.parent / "auth"
        credentials_file_path = google_auth_dir / "credentials.json"
        if not google_auth_dir.exists():
            os.makedirs(google_auth_dir)
        token_file_path = google_auth_dir / "token.json"

        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if token_file_path.exists():
            creds = Credentials.from_authorized_user_file(str(token_file_path), scopes)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(str(credentials_file_path), scopes)
                with open(credentials_file_path, "r+") as cred_file:
                    cred_json = json.load(cred_file)
                    cred_file.seek(0)
                    cred_file.truncate()
                    json.dump(cred_json, cred_file, indent=2)

                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(token_file_path, "w") as token:
                json.dump(json.loads(creds.to_json()), token, indent=2)

        try:
            service = build("sheets", "v4", credentials=creds)

            sheet_range = f"{tab_name}!{starting_range_col}{starting_range_row}:{ending_range_col}{ending_range_row}"

            # Call the Sheets API
            sheet = service.spreadsheets()
            # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get
            result = sheet.values().get(
                spreadsheetId=sheet_id,
                range=sheet_range,
                valueRenderOption="UNFORMATTED_VALUE",
                dateTimeRenderOption="FORMATTED_STRING"
            ).execute()
            values = result.get("values", [])

            if not values:
                print("No data found.")
            else:
                options.display.float_format = f"{{:.{RD}f}}".format

                cleaned_data = []
                for row in values:
                    if row and row[0] != "":
                        cleaned_data.append(
                            [str.replace(cell, "\n", " ") if isinstance(cell, str) else cell for cell in row]
                        )

                raw_df = DataFrame(cleaned_data[1:], columns=cleaned_data[0])
                raw_df.index += 1

                taxable_tx_types = set(raw_df[raw_df["Taxable Event Tx #"] != "--"]["Tx Type"].tolist())
                # print(raw_df.head(2).to_string())

                col_name_mapping = OrderedDict([
                    ("Tx Type", "tx_type"),
                    ("Tx Date", "tx_timestamp"),
                    ("Tx Cost", "fiat_value"),
                    ("Fee", "fiat_tx_fee"),
                    ("Currency (FROM)", "currency_in"),
                    ("Daily Avg. (FROM)", "currency_in_fiat_price"),
                    # ("Tx Volume (FROM)", "currency_in_volume"),
                    ("Currency (TO)", "currency_out"),
                    ("Daily Avg. (TO)", "currency_out_fiat_price"),
                    # ("Tx Volume (TO)", "currency_out_volume"),
                    ("Taxable Event Tx #", "tx_taxable")
                ])

                self.data = raw_df[col_name_mapping.keys()].copy()

                for i, col_name in enumerate(self.data.columns, start=1):
                    if col_name in col_name_mapping.keys():
                        self.data_col_index_map[col_name_mapping[col_name]] = i

                self.data.columns = self.data_col_index_map.keys()
                self.data["tx_timestamp"] = self.data["tx_timestamp"].apply(
                    lambda x: to_datetime(x, format="%m/%d/%Y")
                )
                self.data["fiat_value"] = self.data["fiat_value"].apply(
                    lambda x: abs(float(Decimal(x)))
                )
                self.data["fiat_tx_fee"] = self.data["fiat_tx_fee"].apply(
                    lambda x: abs(float(Decimal(x)))
                )
                self.data["currency_in"] = self.data["currency_in"].str.upper()
                self.data["currency_in_fiat_price"] = self.data["currency_in_fiat_price"].apply(
                    lambda x: abs(float(Decimal(x)))
                )
                self.data["currency_out"] = self.data["currency_out"].str.upper()
                self.data["currency_out_fiat_price"] = self.data["currency_out_fiat_price"].apply(
                    lambda x: abs(float(Decimal(x)))
                )
                self.data["tx_taxable"] = self.data["tx_taxable"] != "--"
                self.data["currency_in_volume"] = self.data["fiat_value"] / self.data["currency_in_fiat_price"]
                # self.data.loc[
                #     self.data["currency_in_fiat_price"] > 0,
                #     "currency_in_volume"
                # ] = self.data["fiat_value"] / self.data["currency_in_fiat_price"]
                self.data_col_index_map["currency_in_volume"] = max(self.data_col_index_map.values()) + 1

                self.data["currency_out_volume"] = self.data["fiat_value"] / self.data["currency_out_fiat_price"]
                # self.data.loc[
                #     self.data["currency_out_fiat_price"] > 0,
                #     "currency_out_volume"
                # ] = self.data["fiat_value"] / self.data["currency_out_fiat_price"]
                self.data_col_index_map["currency_out_volume"] = max(self.data_col_index_map.values()) + 1

                self.data["description"] = ""
                self.data.loc[
                    (self.data.tx_type.isin(taxable_tx_types))
                    &
                    (~self.data.tx_type.isin(["BUY", "SELL", "TRADE"])),
                    "description"
                ] = self.data["tx_type"]
                self.data.loc[self.data["description"] == "", "description"] = None
                self.data_col_index_map["description"] = max(self.data_col_index_map.values()) + 1

                self.data.loc[
                    (self.data.tx_type.isin(taxable_tx_types))
                    &
                    (~self.data.tx_type.isin(["BUY", "SELL", "TRADE"])),
                    "tx_type"
                ] = "TRANSACT"

        except HttpError as err:
            print(err)
