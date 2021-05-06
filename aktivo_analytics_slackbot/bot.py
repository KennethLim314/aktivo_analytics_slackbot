import os
import sqlite3

from datetime import datetime, timezone, timedelta
from jinja2 import Environment, FileSystemLoader


def initialize_database(path):
    conn = None
    try:
        conn = sqlite3.connect(path)
        print(conn)
    except:
        raise
    finally:
        conn.close()


def _generate_df_html(df, outpath):
    root = os.path.dirname(os.path.abspath(__file__))
    templates_dir = os.path.join(root, 'templates')
    env = Environment(loader=FileSystemLoader(templates_dir))
    template = env.get_template('table.html')
    context = {"cols": df.columns, "rows": map(lambda x: x[1], df.iterrows())}
    out_html = template.render(**context)
    with open(outpath, "w") as f:
        f.write(out_html)


def dump_df_png(df, script_path, out_path):
    """Dumps the output data to a dataframe

    Args:
        df (TYPE): Description
    """

    _generate_df_png(df, "temp.html")
    try:
        os.system(f"{script_path} ./temp.html, -o {out_path}")
    except:
        raise
    finally:
        os.remove("temp.html")


def generate_slack_message(text):
    return {
        "channel": "test_channel",
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        ],
    }


class AnalyticsCSUpdater:
    def __init__(self, bq_client, wk2png_path, target_companies=None):
        """Summary

        Args:
            bq_client (TYPE): Authenticated bigquery client
            wk2png_path (TYPE): Path to the webkit2png script
            target_companies (list, optional): Company IDs to be summarized
        """
        self.bq_client = bq_client
        self.wk2png_path = wk2png_path
        self.target_companies = target_companies
        company_name_map = self.bq_client.query(
            """
            SELECT company_id, company_name
            FROM `aktivophase2pushnotification.integrated_reporting_precomp.company_name_map`
            """
        ).to_dataframe()
        self.company_name_map = {i.company_id: i.company_name for idx, i in company_name_map.iterrows()}

    def _get_datum(self, date, company_id):
        """Gets the indvidual user data column for a given company at a specific date

        Args:
            date (TYPE): Description
            company_id (TYPE): Description

        Returns:
            TYPE: Description
        """

        query = f'''
        SELECT * FROM `aktivophase2pushnotification.integrated_reporting_precomp.cs_interative_compiled`
        WHERE end_date = DATE("{date.strftime("%Y-%m-%d")}")
        AND company_id = "{company_id}"
        '''
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

    def dump_data(self, data_df, outpath, format="png"):
        """Dumps the resultant output dataframe to a format of our choice

        Args:
            data_df (TYPE): Description

        Deleted Parameters:
            data (TYPE): Description
        """
        if format == "png":
            dump_df_png(data_df, self.wk2png_path, outpath)

        else:
            raise Exception("Unsupported format")

    def generate_message_payload(self, date):
        template = generate_slack_message("Test Message")
        return template


if __name__ == "__main__":
    import pandas as pd

    _df = pd.DataFrame({"col1": [1, 2, 3], "col2": [1, 2, 3]})
    print(_df.columns)
    _generate_df_html(_df, "test.html")
