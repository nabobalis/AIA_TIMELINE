"""
This script needs requests, pandas and beautifulsoup4 installed to run.
"""
import os
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import requests

DATETIMEFORMAT = "%Y-%m-%dT%H:%M:%S.%f"
BASE = {
    "spacecraft_night": {
        "URL": "https://aia.lmsal.com/public/sdo_spacecraft_night.txt",
        "DTFORMAT": "%y-%j-%H:%M:%S",  # "YY-DOY-HH:MM:SS"
        "SKIPROWS": [0, 1, 2],
        "RANGE": None,
        "SCRAPE": False,
    },
    "jsocobs_info": {
        "URL": "https://aia.lmsal.com/public/jsocobs_info.html",
        "fURL": "https://aia.lmsal.com/public/jsocobs_info{}.html",
        "SKIPROWS": None,
        "DTFORMAT": None,
        "RANGE": range(10, 21),
        "SCRAPE": False,
    },
    # This site has a whole range of text files and its easier to scrape the urls that way.
    # Assumption is that each text file on this page has the same structure
    "jsocinst_calibrations": {
        "URL": "https://aia.lmsal.com/public/jsocinst_calibrations.html",
        "SKIPROWS": [0],
        "DTFORMAT": "%d-%b-%y %H:%M:%S",  # 06-Apr-10 21:11:55
        "RANGE": None,
        "SCRAPE": True,
    },
}


def _process_time(data: pd.DataFrame, dt_format: str):
    data.iloc[:, 0] = data.iloc[:, 0].apply(lambda x: datetime.strptime(x, dt_format))
    try:
        data.iloc[:, 1] = data.iloc[:, 1].apply(lambda x: datetime.strptime(x, dt_format))
    except Exception:
        # Work around that the second collection of text files only have the end hour/minute
        # and not the full date
        dates = data.iloc[:, 0].apply(lambda x: str(pd.Timestamp(x).date()) + " ").values
        new_dates = dates + data.iloc[:, 1].values
        data.iloc[:, 1] = [datetime.strptime(x, "%Y-%m-%d %H:%M:%S") for x in new_dates]


def process_txt(url: str, skiprows: list, dt_format: str, data: pd.DataFrame):
    new_data = pd.read_fwf(url, skiprows=skiprows)
    new_data["Instrument"] = "AIA" if "AIA" in url else "HMI" if "HMI" in url else "SDO"
    # Assume first line is the best description of current file
    new_data["Comment"] = requests.get(url).text.splitlines()[0]
    # Transform to nice timestamps
    _process_time(new_data, dt_format)
    # We want to keep the original columns we are after intact.
    extra_idx = list(range(len(new_data.columns) - 4))
    # Assume that the order of each file is the same
    new_data.columns = ["Start Time", "End Time", *extra_idx, "Instrument", "Comment"]
    new_data = new_data.drop(columns=extra_idx)
    data = pd.concat([data, new_data], ignore_index=True)
    return data


def _format_date_html(date: str, year: str, _hack=None):
    # Blank
    if not date:
        date = None
    # Only date
    elif len(date) in [4, 5]:
        # Deal with only times with a hack
        if "/" not in date:
            date = pd.Timestamp(str(_hack.date()) + " " + date)
        else:
            date = pd.Timestamp(f"{year}-{date}")
    # Year missing
    elif len(date) in [9, 10, 11, 12]:
        date = date.split(" ")
        date = pd.Timestamp(date[0] + f"/{year} " + date[1])
    # Year present
    elif len(date) in [15, 16]:
        date = pd.Timestamp(date)
    # Multiple times
    # TODO: For now, just take the first one
    else:
        idx = len(date) // (len(date) // 10)
        date = pd.Timestamp(f"{year}-{date[:idx]}")
    return date


def _clean_date(date: str):
    date = (
        " ".join(date.split())
        .replace(".", ":")
        .replace("UT", "")
        .replace(" TBD", "")
        .replace("ongoing", "")
        .replace("AIA", "")
        .replace("HMI", "")
        .replace("- 21:00", "")
    )
    return date


def process_html(url: str, data: pd.DataFrame):
    year = url.split("info")[1].split(".")[0]
    request = requests.get(url)
    soup = BeautifulSoup(request.text, "html.parser")
    # First table is the two lists
    table = soup.find_all("table")[1]
    rows = table.find_all("tr")
    # Ignore the header row
    for row in rows[1:]:
        text = row.text.strip().split("\n")
        # First column is the start time
        #    Can have multiple times
        # Second column is the end time
        #   Can be be blank
        # Third column is the event
        # Fifth column is the AIA Description
        # Eighth column is the HMI Description
        comment = text[2].strip() or text[4].strip() or text[7].strip()
        instrument = "SDO" if text[2].strip() else "AIA" if text[4].strip() else "HMI"
        start_date = _clean_date(text[0])
        end_date = _clean_date(text[1]) if len(text[1]) > 1 else ""
        start_date = _format_date_html(start_date, year)
        end_date = _format_date_html(end_date, year, start_date)
        data.loc[len(data.index)] = [start_date, end_date, instrument, comment]
    return data


def scrape_url(url: str):
    base_url = os.path.dirname(url)
    request = requests.get(url)
    soup = BeautifulSoup(request.text, "html.parser")
    urls = []
    for link in soup.find_all("a"):
        a_url = link.get("href")
        if a_url and "txt" in a_url:
            urls.append(base_url + "/" + a_url)
    return urls


data = pd.DataFrame(columns=["Start Time", "End Time", "Instrument", "Comment"])
for block in BASE.values():
    if block["SCRAPE"]:
        urls = scrape_url(block["URL"])
    else:
        if block["RANGE"] is None:
            urls = [block["URL"]]
        else:
            urls = [block["fURL"].format("20" + str(i)) for i in block["RANGE"]]
    urls = sorted(urls)
    for url in urls:
        if "txt" in url:
            data = process_txt(url, block["SKIPROWS"], block["DTFORMAT"], data)
        else:
            data = process_html(url, data)


data = data.sort_values("Start Time")
data.to_csv("timeline.csv", index=False)
data.to_csv("timeline.txt", sep="\t", index=False)
