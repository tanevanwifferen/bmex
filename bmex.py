"""
bmex.py

A script to download and store historical data (quotes + trades) from BitMEX. 

Copyright (c) 2019, Diogo Flores.
License: MIT
"""

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


def _validate_dates(start: dt, end: dt):
    """
    Validates start and end dates prior to polling data from BitMEX servers.
    """

    # Earliest date of data available.
    min_date = dt(2014, 11, 22)
    today = dt.today()

    if end < start:
        raise Exception("End-date can't be earlier than start-date.")

    if start < min_date:
        raise Exception("Start-date can't be earlier than {min_date.date()}")

    if end > today:
        end = today

    return start, end


def _validate_symbols(symbols: set):
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


def _make_dirs(symbols: set, save_to: str = None):
    """
    Creates a base directory and one sub-directory for each symbol, to be
    populated with historical data.
    """

    base = "BITMEX"
    path = save_to

    if path:
        if not os.path.exists(path):
            sys.exit("\nError: The path you provided does not exist.\n")
    else:
        path = os.getcwd()

    if not os.path.isdir(f"{path}/{base}"):
        try:
            os.mkdir(f"{path}/{base}")
        except PermissionError:
            sys.exit(f"\nError: You don't have permissions to write on {path}\n")

    for sym in symbols:
        if not os.path.isdir(f"{path}/{base}/{sym}"):
            os.mkdir(f"{path}/{base}/{sym}")

    return base, path


def _unzip(current: str, r):
    """
    Unzip downloaded .tar.gz file and parse the data inside.
    """
    with open(current, "wb") as fp:
        fp.write(r.content)

    with gzip.open(current, "rb") as fp:
        data = fp.read()

    with open(current, "wb") as fp:
        fp.write(data)


def _store(start: str, symbols: set, channel: str, path: str, base: str):
    """
    Stores the data as .csv files on a pre-defined (see README.md) directory structure.
    """
    c = start.strftime("%Y%m%d")  # Same as 'current' - saves passing one more arg.
    new = True

    with open(c, "r") as inp:
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

                _file = f"{location}/{c[:4]}-{c[4:6]}-{c[6:]}.csv"
                # If the file already exists, remove it before creating a new one
                # and appending to it.
                # This is a safety measure to ensure data integrity, in case the
                # program is run with the same start and end dates multiple times.
                if new:
                    if os.path.exists(_file):
                        os.remove(_file)
                    new = False

                with open(_file, "a") as out:
                    write = csv.writer(out)
                    write.writerow(row)
    os.remove(c)


def poll_data(start: dt, end: dt, symbols: set, channel: str, save_to: str = None):
    """
    Polls data from BitMEX servers.
    """

    start, end = _validate_dates(start, end)
    _validate_symbols(symbols)

    # This function "should" be called from _store(), but since _store() is called
    # N (days) times, I placed the function call here for performance sake.
    base, path = _make_dirs(symbols, save_to)

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
                    if r.status_code == 404:
                        # Data for today or yesterday might yet not be available.
                        today = dt.today()
                        yesterday = today - timedelta(1)
                        if (
                            start.date() == today.date()
                            or start.date() == yesterday.date()
                        ):
                            return f"Failed to download: {start.date()} - data not (yet) available."
                    r.raise_for_status()
                print(f"{r.status_code} error processing: {start.date()} - retrying.")
                time.sleep(10)

        _unzip(current, r)
        _store(start, symbols, channel, path, base)

        print(f"Processed {channel}s: {str(start)[:10]}")
        start += timedelta(days=1)

    return "Success - all data downloaded and stored."


def main(args):
    start = dt.strptime(args.start, "%Y-%m-%d")
    end = dt.strptime(args.end, "%Y-%m-%d")
    save_to = args.save_to

    # Remove possible duplicates.
    symbols = set(args.symbols)
    channels = set(args.channels)

    report = {}
    if "trades" in channels:
        report["trades"] = poll_data(start, end, symbols, "trade", save_to)
    if "quotes" in channels:
        report["quotes"] = poll_data(start, end, symbols, "quote", save_to)

    print("-" * 80)
    print("Finished.\n")
    print("Report")
    print("------")
    for k, v in report.items():
        print(f"{k}: {v}")
    print()


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Download and store BitMEX historical data."
    )
    parser.add_argument(
        "--symbols", nargs="+", required=True, help="Symbols/indices to download."
    )
    parser.add_argument(
        "--channels",
        nargs="+",
        required=True,
        choices=["quotes", "trades"],
        help="Choose between 'quotes' or 'trades' channel. Both are allowed.",
    )
    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="From when to retrieve data. Format: YYYY-MM-DD",
    )
    parser.add_argument(
        "--end",
        type=str,
        required=True,
        help="Until when to retrieve data. Format: YYYY-MM-DD",
    )
    parser.add_argument(
        "--save_to",
        type=str,
        help="Provide a full path for where to store the retrieved data. (optional)",
    )

    arguments = parser.parse_args()
    return arguments


if __name__ == "__main__":
    arguments = parse_arguments()
    main(arguments)
