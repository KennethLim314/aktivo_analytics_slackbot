import os
import sqlite3

from datetime import timedelta
from jinja2 import Environment, FileSystemLoader
def file2bin(file):
    """Converts a file or filepath to a file to binary data

    Args:
        file (TYPE): Description
    """
    try:
        blob_data = file.read()
    except AttributeError:
        with open(file, "rb") as f:
            blob_data = f.read()
    return blob_data


def initialize_database(path, purge=False):
    """Initializes the run_tracking database, creating the tables if necessary

    Args:
        path (TYPE): Description
    """
    if purge and os.path.exists(path) and "memory" not in path:
        logger.info(f"Removing existing database at {path}")
        os.remove(path)
    conn = None
    try:
        conn = sqlite3.connect(path)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS runs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date DATETIME NOT NULL,
                target_date DATE NOT NULL,
                output_table BLOB NOT NULL,
                output_image BLOB NOT NULL,
                run_type STRING
            );
            """
        )

        conn.commit()
    except:
        conn.close()
        raise
    return conn


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
        df (TYPE): Dataframe to be dumped
        script_path (TYPE): Path to the the webkit2png script used for dataframe export
        out_path (TYPE): Output path for the resultant image

    Returns:
        out_path: Parth of the output dataframe
    """

    _generate_df_html(df, "temp.html")
    try:
        cmd = f"python {script_path} ./temp.html -o {out_path}"
        print(f"Generating Image with command {cmd}")
        subprocess.run(cmd, check=True)
        assert os.path.exists(out_path)
        print("Successfully generated image")
    except:
        raise
    finally:
        try:
            # os.remove("temp.html")
            pass
        except PermissionError:
            time.sleep(2)
            os.remove("temp.html")
    return out_path


class AnalyticsCSUpdater:
    def __init__(
        self, bq_client, slack_client, wk2png_path, sql_conn, target_companies=None, target_channel="test_channel"
    ):
        """Summary

        Args:
            bq_client (TYPE): Authenticated bigquery client
            slack_client (TYPE): Authneticated Slack client
            wk2png_path (TYPE): Path to the webkit2png script
            sql_conn (TYPE): connection to a sqlite database
            target_companies (list, optional): Company IDs to be summarized
            target_channel (str, optional): Which channel should this bot post to
        """
        self.bq_client = bq_client
        self.slack_client = slack_client
        self.wk2png_path = wk2png_path

        self.sql_conn = sql_conn

        self.target_channel = target_channel
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

    def send_message_text(self, text):
        """Sends a simple plain text message

        Args:
            text (TYPE): Description
        """
        params = {
            "channel": self.target_channel,
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": text}},
            ],
        }
        print(f"Sending message with params: {params}")
        self.slack_client.chat_postMessage(**params)

    def send_message_data_image(self, data_df, title):
        """Sends a message

        Args:
            data_df (TYPE): Description
            title (TYPE): Description

        Returns:
            TYPE: Description
        """
        image_path = dump_df_png(data_df, self.wk2png_path, "./temp.png")
        with open(image_path, "rb") as f:
            params = {
                "channels": self.target_channel,
                "filename": "temp.png",
                "file": f,
                "initial_comment": title,
            }
            print(f"Sending with params: {params}")
            self.slack_client.files_upload(**params)
        print(f"Successfully sent image")

        # Log the run in DB, dumping all data
        target_date = data_df.end_date[0]
        run_date = datetime.now()
        image_blob = file2bin(image_path)
        _tempfile = StringIO()
        data_df.to_csv(_tempfile)
        table_blob = file2bin(_tempfile)
        insert_query = """INSERT INTO runs
        (run_date, target_date, output_table, output_image) VALUES (?, ?, ?, ?)
        """
        data_tuple = (run_date, target_date, table_blob, image_blob)
        print(data_tuple[:2])
        self.sql_conn.execute(insert_query, data_tuple)
        self.sql_conn.commit()
        return


if __name__ == "__main__":
    import pandas as pd

    _df = pd.DataFrame({"col1": [1, 2, 3], "col2": [1, 2, 3]})
    print(_df.columns)
    _generate_df_html(_df, "test.html")
