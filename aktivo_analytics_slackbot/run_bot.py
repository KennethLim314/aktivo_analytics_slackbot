import argparse
from bot import AnalyticsCSUpdater, initialize_database
from slack import WebClient
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from datetime import datetime


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("slack_token_path")
    parser.add_argument("bigquery_auth_path")
    parser.add_argument("--existing_db", default="./runs.sqlite3")
    parser.add_argument("--w2p_path", default="./webkit2png", help="Path to the webkit2png library")
    args = parser.parse_args()
    with open(args.slack_token_path) as f:
        slack_token = f.read().strip()
    slack_client = WebClient(slack_token)

    credentials = service_account.Credentials.from_service_account_file(args.bigquery_auth_path)
    bq_client = bigquery.Client(credentials=credentials)

    # Initialize the client
    if not os.path.exists(args.existing_db):
        initialize_database(args.existing_db)

    target_companies = [
        "603de62548d6dc001315dfb4",
        "5b3eefcc04802c000ff7c91e",
        "603a1e1248d6dc001315a2c7",
        "600690d96bbcf900128a87d8",
        "5ed45e746be1a70012b152b7"
,    ]
    bot = AnalyticsCSUpdater(bq_client, target_companies)
    current_date = datetime.now(timezone.utc)
    print(bot.get_data(current_date))
    message = bot.generate_message_payload(1)
    slack_client.chat_postMessage(**message)
