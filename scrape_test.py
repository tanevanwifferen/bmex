from datetime import datetime as dt
from datetime import timedelta
import csv
import gzip
import os
import time
import requests

# https://public.bitmex.com/?prefix=data/trade/
endpoint = "https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/{}/{}.csv.gz"


def get_data(start, end, symbols, channel="trade"):
    """
    Pulls data and creates the necessary directories to store it. 
    """
    base = "BITMEX"
    path = os.getcwd()

    if not os.path.isdir(f"{path}/{base}"):
        os.mkdir(base)

    for sym in symbols:
        if not os.path.isdir(f"{path}/{base}/{sym}"):
            os.mkdir(f"{path}/{base}/{sym}")

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

        print(f"Processed: {str(start)[:10]}")
        os.remove(current)
        start += timedelta(days=1)


# 2014-11-12 is the first day of data.
start = dt(2018, 10, 30)
end = dt(2018, 11, 1)
symbols = ["XBTUSD", "ETHUSD"]

trades = True
quotes = False

if trades:
    get_data(start, end, symbols, channel="trade")

if quotes:
    get_data(start, end, symbols, channel="quote")
