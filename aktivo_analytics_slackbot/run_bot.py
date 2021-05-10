"""
Things to account for: Rerunning
"""
import argparse
from bot import AnalyticsCSUpdater, initialize_database
from slack import WebClient
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG, format="[%(levelname)s][%(filename)s:%(lineno)s] %(message)s")
logger = logging.getLogger("run_bot")


def get_output_date_range(conn, max_days=4, current_date=None, min_start_date=None):
    """Summary
    Args:
        conn (TYPE): Description
        max_days (int, optional): Number of days of data to present at once
        current_date (None, optional): Description
        min_start_date (TYPE, optional): Minimum to start from

    Returns:
        TYPE: Description
    """
    if current_date is None:
        current_date = datetime.now(tz=timezone.utc).date()
    if min_start_date is None:
        min_start_date = datetime(1000, 1, 1).date()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT MAX(target_date) FROM runs;
    """
    )

    rows = cur.fetchall()
    logger.debug(f"Rows: {rows}")
    # No previous run, return the min_start_date instead
    if not rows[0][0]:
        start = min_start_date
    else:
        start = max(parse(rows[0][0]).date() +timedelta(days=1), min_start_date)
    end = min(start + timedelta(days=max_days), current_date)
    return start, end


def daterange(start_date, end_date):
    cur_date = start_date
    while True:
        if cur_date >= end_date:
            return
        yield cur_date
        cur_date += timedelta(days=1)


def run_bot(bot, start_date, end_date):
    # Make sure an end_date exists
    # What if there's no data for that date for some companies (ignore for now)
    for target_date in daterange(start_date, end_date):
        data = bot.get_data(target_date)
        if data.shape[0] < 1:
            logger.warning(f"No data found for date={end_date.strftime('%Y-%m-%d')}")
            continue
        # Rename the data to make it more pleasant to the eye
        wtd_start = start_date - timedelta(days=start_date.day)
        mtd_start = start_date + timedelta(days=-start_date.day + 1)
        bot.send_message_data_image(
            data,
            (
                f"Application User Data for *{target_date.strftime('%Y-%m-%d')}*\n\n"
                f"Week to Date (WTD) taken into account: "
                f"*{wtd_start.strftime('%Y-%m-%d')}* - *{target_date.strftime('%Y-%m-%d')}*\n"
                f"Month to Date (MTD) taken into account: "
                f"*{mtd_start.strftime('%Y-%m-%d')}* - *{target_date.strftime('%Y-%m-%d')}*\n"
            ),
        )


def run_bot_test(bot, start_date, end_date, data=None):
    if data is None:
        data = pd.DataFrame({"end_date": [end_date, end_date, end_date], "col2": [1, 2, 3]})
    for target_date in daterange(min(start_date, data.end_date.min()), end_date):
        bot.send_message_data_image(data, f"*TEST:* Application User Data for *{target_date.strftime('%Y-%m-%d')}*")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("slack_token_path")
    parser.add_argument("bigquery_auth_path")
    parser.add_argument("--data_dir", default="./analytics_slackbot_data")
    parser.add_argument(
        "--purge", default=False, action="store_true", help="Reinitialize the database, wiping all data"
    )
    parser.add_argument(
        "--companies",
        default=",".join(
            [
                "603de62548d6dc001315dfb4",
                "5b3eefcc04802c000ff7c91e",
                "603a1e1248d6dc001315a2c7",
                "600690d96bbcf900128a87d8",
                "5ed45e746be1a70012b152b7",
            ]
        ),
        help="comma-separated string of company IDs to get data for",
    )
    parser.add_argument(
        "--start_date",
        default=None,
        help="From which date to start getting data from. Format should be YY-MM-DD",
    )
    parser.add_argument(
        "--dryrun", default=False, action="store_true", help="Dryrun without the long. Still writes to database though"
    )
    parser.add_argument(
        "--n_days",
        default=2,
        type=int,
        help="Number of new days of updates to push through at once. This"
        " stops the bot from spamming incessantly if too little data is present",
    )
    parser.add_argument(
        "--target_channel", default="test_channel", help="Which channel should the bot post to by default"
    )
    args = parser.parse_args()

    # Common Issues
    if not args.start_date and not args.dryrun:
        raise Exception("start_date needs to be specified for non-dryrun runs")

    # Initialization of the bot and all the reqs
    with open(args.slack_token_path) as f:
        slack_token = f.read().strip()
    print("Starting Bot")
    # print(slack_token)
    slack_client = WebClient(slack_token)
    credentials = service_account.Credentials.from_service_account_file(args.bigquery_auth_path)
    bq_client = bigquery.Client(credentials=credentials)

    # Initialize the folder structures
    rundata_dir = os.path.join(args.data_dir, "rundata")
    for path in [args.data_dir, rundata_dir]:
        if not os.path.exists(path):
            os.makedirs(path)
    # initialize the database
    db_path = os.path.join(args.data_dir, "runs.sqlite3")
    conn = initialize_database(db_path, purge=args.purge)

    target_companies = args.companies.split(",")
    bot = AnalyticsCSUpdater(
        bq_client,
        slack_client,
        sql_conn=conn,
        target_companies=target_companies,
        target_channel=args.target_channel
    )

    # Execution of the bot
    # First define the start and end dates
    start_date = args.start_date
    if start_date:
        start_date = parse(start_date).date()
    logger.debug(f"raw start_date: {start_date}")
    start_date, end_date = get_output_date_range(conn, min_start_date=start_date, max_days=args.n_days)
    logger.info(f"Generating data for daterange: {start_date}, {end_date}")
    if start_date == end_date:
        logger.info("Current posts already up to date")
        quit()
    if args.dryrun:
        run_bot_test(
            bot,
            start_date,
            end_date,
        )
    else:
        run_bot(bot, start_date, end_date)
