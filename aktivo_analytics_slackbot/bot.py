from slack import WebClient
import os
import sqlite3
import argparse
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timezone, timedelta




def initialize_database(path):
    conn = None
    try:
        conn = sqlite3.connect(path)
        print(conn)
    except:
        raise
    finally:
        conn.close()

def dump_df(df):
    pass


def generate_template(text):
    return {
        "channel": "test_channel",
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        ],
    }


class AnalyticsCSUpdater:
    def __init__(self, bq_client, target_companies=None):
        self.bq_client = bq_client
        self.target_companies = target_companies
        company_name_map = self.bq_client.query("""
            SELECT company_id, company_name
            FROM `aktivophase2pushnotification.integrated_reporting_precomp.company_name_map`
        """).to_dataframe()
        self.company_name_map = {i.company_id: i.company_name for idx, i in company_name_map.iterrows()}

    def _get_datum(self, date, company_id):
        query = f"""
        SELECT * FROM `aktivophase2pushnotification.integrated_reporting_precomp.cs_interative_compiled`
        WHERE end_date = DATE("{date.strftime("%Y-%m-%d")}")
        AND company_id = "{company_id}"
        """
        res = self.bq_client.query(query).to_dataframe()
        return res

    def get_data(self, date):
        holder = []
        for company_id in self.target_companies:
            company_name = self.company_name_map[company_id]
            print(f"Fetching data for company={company_name}")
            _ = self._get_datum(date, company_id)
            _ = _.rename(columns={"n_users": company_name})
            holder.append(_)
        # print(holder)
        base = holder[0].iloc[:, 1:4]
        for i in holder[1:]:
            base = base.merge(i.iloc[:, 1:4], on=["end_date", "subcategory"])
        return base

    def dump_data(self, data):
        pass

    def generate_message_payload(self, date):
        template = generate_template("Test Message")
        return template


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("slack_token_path")
    parser.add_argument("bigquery_auth_path")
    parser.add_argument("--existing_db", default="./runs.sqlite3")
    args = parser.parse_args()
    with open(args.slack_token_path) as f:
        slack_token = f.read().strip()
    slack_client = WebClient(slack_token)

    credentials = service_account.Credentials.from_service_account_file(
        args.bigquery_auth_path
    )
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
    ]
    bot = AnalyticsCSUpdater(bq_client, target_companies)
    current_date = datetime.now(timezone.utc)
    print(bot.get_data(current_date))
    message = bot.generate_message_payload(1)
    slack_client.chat_postMessage(**message)
