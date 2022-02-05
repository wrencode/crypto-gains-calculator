import json
import os
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pandas import DataFrame

# If modifying these scopes, delete the file token.json
# scopes can be found here: https://developers.google.com/identity/protocols/oauth2/scopes
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# The ID and range of a sample spreadsheet.
# noinspection SpellCheckingInspection
SAMPLE_SPREADSHEET_ID = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
SAMPLE_RANGE_NAME = "Class Data!A2:E"

SAMPLE_SPREADSHEET_ID = "10Fco8GhmN1LbGb9RfDCGTosEsZGGp3Yb9mkOW39al0k"
SAMPLE_RANGE_NAME = "transactions!A:Y"


def main():
    """Shows basic usage of the Sheets API & prints values from a sample spreadsheet.
    """

    google_auth_dir = Path(__file__).parent.parent / "auth"
    credentials_file_path = google_auth_dir / "credentials.json"
    if not google_auth_dir.exists():
        os.makedirs(google_auth_dir)
    token_file_path = google_auth_dir / "token.json"

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if token_file_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_file_path), SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_file_path), SCOPES)
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

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME).execute()
        values = result.get("values", [])

        if not values:
            print("No data found.")
        else:
            cleaned_data = []
            for row in values:
                if row and row[0] != "":
                    cleaned_data.append([str.replace(cell, "\n", " ") for cell in row])
            df = DataFrame(cleaned_data[1:], columns=cleaned_data[0])
            df.index += 1
            # print(set(df["Tx Type"].tolist()))
            print(df.to_string())

    except HttpError as err:
        print(err)


if __name__ == "__main__":
    main()
