import os
import sqlite3

from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timezone, timedelta
from jinja2 import Environment, FileSystemLoader
import os


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
        company_name_map = self.bq_client.query(
            """
            SELECT company_id, company_name
            FROM `aktivophase2pushnotification.integrated_reporting_precomp.company_name_map`
        """
        ).to_dataframe()
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
    import pandas as pd

    _df = pd.DataFrame({"col1": [1, 2, 3], "col2": [1, 2, 3]})
    print(_df.columns)
    _generate_df_html(_df, "test.html")
