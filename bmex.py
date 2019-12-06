import argparse
import csv
from datetime import datetime as dt
from datetime import timedelta
import gzip
import os
import requests
import sys
import time

# https://public.bitmex.com/?prefix=data/trade/
endpoint = "https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/{}/{}.csv.gz"


def _validate_dates(start, end):
    """
    Validates start and end times for successfuly polling data from BitMEX servers.
    """

    # Earliest date of data available.
    min_date = dt(2014, 11, 22)
    today = dt.today()

    if end < start:
        raise Exception("End-date can't be earlier than start-date.")

    if start < min_date:
        raise Exception("Start-date can't be earlier than {min_date}")

    if end > today:
        end = today

    return start, end


def _validate_symbols(symbols):
    """
    Validates that each symbol/index exists/existed on BitMEX.
    """

    r = requests.get(
        "https://www.bitmex.com/api/v1/instrument?count=500&reverse=false"
    ).json()

    valid = [x["symbol"] for x in r]
    not_valid = [symb for symb in symbols if symb not in valid]

    if not_valid:
        sys.exit(f"Not valid symbol(s): {not_valid}.")


def get_data(start, end, symbols, channel="trade"):
    """
    Polls data and creates the necessary directories to store it. 
    """

    start, end = _validate_dates(start, end)
    _validate_symbols(symbols)

    base = "BITMEX"
    path = os.getcwd()

    if not os.path.isdir(f"{path}/{base}"):
        os.mkdir(base)

    for sym in symbols:
        if not os.path.isdir(f"{path}/{base}/{sym}"):
            os.mkdir(f"{path}/{base}/{sym}")

    print("-" * 80)
    print(f"Start processing {channel}s:\n")
    while start <= end:
        current = start.strftime("%Y%m%d")
        count = 0
        while True:
            r = requests.get(endpoint.format(channel, current))
            if r.status_code == 200:
                break
            else:
                count += 1
                if count == 10:
                    r.raise_for_status()
                print(f"Error processing: {start} - {r.status_code}, retrying.")
                time.sleep(10)

        with open(current, "wb") as fp:
            fp.write(r.content)

        with gzip.open(current, "rb") as fp:
            data = fp.read()

        with open(current, "wb") as fp:
            fp.write(data)

        c = current
        with open(current, "r") as inp:
            reader = csv.reader(inp)
            for row in reader:
                # Pandas couldn't parse the dates - The next line fixes that.
                row[0] = row[0].replace("D", " ", 1)

                if row[1] in symbols:
                    location = (
                        f"{path}/{base}/{row[1]}/{channel}s/{start.year}/{start.month}"
                    )
                    if not os.path.isdir(location):
                        os.makedirs(location)
                    with open(f"{location}/{c[:4]}-{c[4:6]}-{c[6:]}.csv", "w+") as out:
                        write = csv.writer(out)
                        write.writerow(row)

        print(f"Processed {channel}s: {str(start)[:10]}")
        os.remove(current)
        start += timedelta(days=1)


def main(args):
    start = dt.strptime(args.begin, "%Y-%m-%d")
    end = dt.strptime(args.end, "%Y-%m-%d")

    # Some protection for possible duplicates.
    symbols = set(args.symbols)
    channels = set(args.channels)

    if "trades" in channels:
        get_data(start, end, symbols, channel="trade")
    if "quotes" in channels:
        get_data(start, end, symbols, channel="quote")

    print("-" * 80)
    print("Finished.\n")


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Download and store BitMEX historical data."
    )
    parser.add_argument(
        "-s",
        "--symbols",
        nargs="+",
        required=True,
        metavar="",
        help="Symbols/indices to download.",
    )
    parser.add_argument(
        "-c",
        "--channels",
        nargs="+",
        required=True,
        metavar="",
        choices=["quotes", "trades"],
        help="Choose between 'quotes' or 'trades' channel. Both are allowed.",
    )
    parser.add_argument(
        "-b",
        "--begin",
        type=str,
        required=True,
        metavar="",
        help="From when to retrieve data. Format: YYYY-MM-DD",
    )
    parser.add_argument(
        "-e",
        "--end",
        type=str,
        required=True,
        metavar="",
        help="Until when to retrieve data. Format: YYYY-MM-DD",
    )

    arguments = parser.parse_args()
    return arguments


if __name__ == "__main__":
    arguments = parse_arguments()
    main(arguments)
