import os
from datetime import datetime
from itertools import product
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

CY_END = int(str(datetime.now().year + 1)[2:])
TIMEFORMATS = [
    "%d-%b-%y %H:%M:%S",  # 06-Apr-10 21:11:55
    "%Y.%m.%d",  # 2010.05.18
    "%y-%j-%H:%M:%S",  # YY-DOY-HH:MM:SS
    "%Y.%m.%d_%H:%M:%S",  # 2010.11.10_06:01:20
    "%d-%b-%Y %H:%M:%S",  # 9-Apr-2010 07:30:00
]
MAP_4 = {
    0: "Roll Maneuvers",
    1: "Momentum Management Maneuvers",
    2: "Station-Keeping Maneuvers",
    3: "Eclipse Season Begins",
    4: "Eclipse Season Ends",
    5: "Battery Voltage Adjustments",
    6: "EVE Crucform Maneuvers",
    7: "EVE FOV & Offpoint Maneuvers",
    8: "GT/PZT Calibrations",
    9: "CCD Bakeout",
    10: "Regulus Offpoint",
    11: "Comet Offpoint",
    12: "Camera Anomaly",
    13: "Transit Ops/Transit",
    14: "IRU Operations",
    15: "load shed",
    16: "Load/Software Anomaly/Error",
    17: "Table Parity Error",
    18: "Misc Instrument Errors Not Listed Above",
    19: "Misc Tests/Special Ops",
}
DATASETS = {
    "spacecraft_night": {
        "URL": "https://aia.lmsal.com/public/sdo_spacecraft_night.txt",
        "SKIPROWS": [0, 1, 2, 3],
    },
    "jsocobs_info": {
        "fURL": "https://aia.lmsal.com/public/jsocobs_info{}.html",
        "RANGE": range(10, CY_END),
    },
    # This site has a whole range of text files and its easier to scrape the urls that way.
    # Assumption is that each text file on this page has the same structure
    "jsocinst_calibrations": {
        "URL": "https://aia.lmsal.com/public/jsocinst_calibrations.html",
        "SKIPROWS": [0],
        "SCRAPE": True,
    },
    "hmi_obs_cov": {
        "fURL": "http://jsoc.stanford.edu/doc/data/hmi/cov2/cov{}.html",
        "RANGE": range(10, CY_END),
        "MONTH_RANGE": range(1, 13),
    },
    "text_block_1": {
        "URL": "./data_1.txt",
    },
    "text_block_2": {
        "URL": "./data_2.txt",
    },
    "text_block_3": {
        "URL": "./data_3.txt",
    },
    "text_block_4": {
        "URL": "./data_4.txt",
    },
}


def _format_date(date: str, year: str, _hack: Optional[datetime] = None) -> pd.Timestamp:
    """
    Formats the given date.

    Parameters
    ----------
    date : str
        Date string from the html file.
    year : str
        The year of the provided dates, it is not present in the date.
    _hack : datetime, optional
        A workaround for some dates, by default None.

    Returns
    -------
    pd.Timestamp
        New date.
    """
    try:
        new_date = pd.Timestamp(date)
        return new_date
    except ValueError:
        pass
    # Only date e., '11/2' assuming month/day
    if len(date) in [4, 5]:
        # Deal with only times with a hack
        if "/" not in date:
            new_date = pd.Timestamp(str(_hack.date()) + " " + date)
        else:
            new_date = pd.Timestamp(f"{year}-{date}")
    # Year missing - e.g., '12/10 18:15'
    elif len(date) in [9, 10, 11, 12]:
        new_date = date.split(" ")
        new_date = pd.Timestamp(new_date[0] + f"/{year} " + new_date[1])
    # Multiple times - e.g., '8/28 20:35 8/14 20:50'
    # TODO: For now, just take the first entry
    else:
        try:
            # This catches 2010.05.01 - 02
            new_date = pd.Timestamp(date.split("-")[0])
        except ValueError:
            idx = len(date) // (len(date) // 10)
            new_date = pd.Timestamp(f"{year}-{date[:idx]}")
    return new_date


def _clean_date(date: str) -> str:
    """
    Removes any non-numeric characters from the date.

    Parameters
    ----------
    date : str
        Date to clean.

    Returns
    -------
    str
        Cleaned date.
    """
    date = (
        " ".join(date.split())
        .replace("UT", "")
        .replace(" TBD", "")
        .replace("ongoing", "")
        .replace("AIA", "")
        .replace("HMI", "")
        # One very specific date
        # 2018-10/16 10:00 - 21:00
        .replace("- 21:00", "")
    )
    return date


def _process_time(data: pd.DataFrame, column: int = 0) -> pd.DataFrame:
    """
    Reformats all the time columns to have a consistent format.

    This modifies the dataframe in place.

    Parameters
    ----------
    data : pd.DataFrame
        The dataframe with timestamps.
    column : int, optional
        The column to process, by default 0.
    """
    for time_format in TIMEFORMATS:
        try:
            data[data.columns[column]] = data.iloc[:, column].apply(lambda x: datetime.strptime(x, time_format))
            return data
        except Exception:
            pass
    else:
        raise ValueError("Could not find a suitable time format")


def _process_data(data: pd.DataFrame, filepath: str) -> pd.DataFrame:
    """
    Certain online text files have no comments or have a comment in the third
    column.

    Parameters
    ----------
    data : pd.DataFrame
        Dataframe to process.
    filepath : str
        Path to the file.

    Returns
    -------
    pd.DataFrame
        Processed dataframe.
    """
    if "AIA" in filepath:
        data["Instrument"] = "AIA"
    elif "HMI" in filepath:
        data["Instrument"] = "HMI"
    else:
        data["Instrument"] = "SDO"
    if "Start Date/Time" in data.columns:
        data.rename(columns={"Start Date/Time": "Start Time"}, inplace=True)
    if "FSN" in data.columns:
        data.rename(columns={"FSN": "Comment"}, inplace=True)
    if "Unnamed: 2" in data.columns:
        data.rename(columns={"Unnamed: 2": "Comment"}, inplace=True)
    if data.columns[-1] == "Comment":
        data["Comment"].fillna(pd.read_fwf(filepath).columns[0])
    else:
        # Assumption that the comment is the first row which pandas turns into a column
        data["Comment"] = pd.read_fwf(filepath).columns[0]
    data = data.loc[:, ["Start Time", "End Time", "Instrument", "Comment"]]
    return data


def _reformat_data(data: pd.DataFrame, filepath: str) -> pd.DataFrame:
    """
    Due to the fact that the text files are not consistent, we need to reformat
    them.

    Parameters
    ----------
    data : pd.DataFrame
        Dataframe to reformat.
    filepath : str
        Path to the file.

    Returns
    -------
    pd.DataFrame
        Reformatted dataframe.
    """
    if "_1" in filepath:
        data["Start Time"] = [None] * len(data)
        data["End Time"] = [None] * len(data)
        for i, row in enumerate(data[0].str.split()):
            data["Start Time"][i] = row[0]
            data["End Time"][i] = row[1]
        data.drop(columns=[0], inplace=True)
        data = data.iloc[:, [1, 2, 0]]
        data.columns = ["Start Time", "End Time", "Comment"]
    elif "_2" in filepath:
        data.columns = ["Start Time", "Comment"]
    elif "_3" in filepath:
        data.columns = ["Start Time", "Comment"]
    elif "_4" in filepath:
        data = data.iloc[:, [1, 0]]
        data.columns = ["Start Time", "Comment"]
        data["Comment"] = data["Comment"].apply(lambda x: MAP_4[x])
    return data


def process_txt(filepath: str, skiprows: Optional[list], data: pd.DataFrame) -> pd.DataFrame:
    """
    Processes a text file.

    Parameters
    ----------
    filepath : str
        File path of the text file.
    skiprows : list, None
        What rows to skip.
    data : pd.DataFrame
        Dataframe to append to.

    Returns
    -------
    pd.DataFrame
        Dataframe with the data from the text file.
    """
    if "http" in filepath:
        new_data = pd.read_fwf(
            filepath,
            header=None if "sdo_spacecraft_night" in filepath else 0,
            skiprows=skiprows,
        )
        new_data = _process_time(new_data)
        if len(new_data.columns) in [2, 3]:
            new_data = _process_data(new_data, filepath)
        elif len(new_data.columns) > 3:
            print(f"Unexpected number of columns for {filepath}, dropping all but first two")
            new_data = new_data.iloc[:, [0, 1]]
            new_data.columns = ["Start Time", "End Time"]
            try:
                new_data = _process_time(new_data, 1)
            except Exception:
                pass
            new_data = _process_data(new_data, filepath)
    else:
        new_data = pd.read_csv(filepath, header=None, sep="    ", skiprows=skiprows, engine="python")
        new_data = _reformat_data(new_data, filepath)
        new_data = _process_time(new_data)
        new_data["Instrument"] = new_data["Comment"].apply(lambda x: "AIA" if "AIA" in x else None)
        new_data["Instrument"] = new_data["Comment"].apply(lambda x: "HMI" if "HMI" in x else None)
    data = pd.concat([data, new_data], ignore_index=True)
    return data


def process_html(url: str, data: pd.DataFrame) -> pd.DataFrame:
    """
    Processes an html file.

    Parameters
    ----------
    url : str
        URL of the html file.
    data : pd.DataFrame
        Dataframe to append to.

    Returns
    -------
    pd.DataFrame
        Dataframe with the data from the html file.
    """
    request = requests.get(url)
    if request.status_code == 404:
        return data
    soup = BeautifulSoup(request.text, "html.parser")
    table = soup.find_all("table")
    # There should be two html tables for this URL
    if len(table) == 1 and "jsocobs_info" in url:
        return data
    table = table[-1]
    rows = table.find_all("tr")
    # TODO: Regex to get the year
    try:
        year = url.split("info")[1].split(".")[0]
    except Exception:
        year = url.split("cov")[2].split(".")[0][:4]
    # These HTML tables are by column and not by row
    if "hmi/cov2/" in url:
        new_rows = rows[0].text.split("\n\n")
        # Time is one single element whereas each event text is a separate element
        dates, text = new_rows[0].strip().split("\n"), new_rows[1:-1]
        # new_rows = [f"{date} {comment}" for date, comment in zip(dates, text)]
        instrument = ["HMI" if "HMI" in new_row else "AIA" if "AIA" in new_row else "SDO" for new_row in text]
        comment = [new_row.replace("\n", " ") for new_row in text]
        start_dates = [(_format_date(_clean_date(date), year)) for date in dates]
        end_dates = [None] * len(dates)
        new_data = pd.DataFrame(
            {"Start Time": start_dates, "End Time": end_dates, "Instrument": instrument, "Comment": comment}
        )
        data = pd.concat([data, new_data])
    else:
        for row in rows[1:]:
            text = row.text.strip().split("\n")
            # First column is the start time
            #   Can have multiple times
            # Second column is the end time
            #   Can be be blank
            # Third column is the event
            # Fifth column is the AIA Description
            # Eighth column is the HMI Description
            comment = text[2].strip() or text[4].strip() or text[7].strip()
            instrument = "SDO" if text[2].strip() else "AIA" if text[4].strip() else "HMI"
            start_date = _clean_date(text[0])
            end_date = _clean_date(text[1]) if len(text[1]) > 1 else "NaT"
            start_date = _format_date(start_date, year)
            end_date = _format_date(end_date, year, start_date)
            new_data = pd.Series(
                {"Start Time": start_date, "End Time": end_date, "Instrument": instrument, "Comment": comment}
            )
            data = pd.concat([data, pd.DataFrame([new_data], columns=new_data.index)]).reset_index(drop=True)
    return data


def scrape_url(url: str) -> list:
    """
    Scrapes a URL for all the text files.

    Parameters
    ----------
    url : str
        URL to scrape.

    Returns
    -------
    list
        List of all the urls scraped.
    """
    base_url = os.path.dirname(url)
    request = requests.get(url)
    soup = BeautifulSoup(request.text, "html.parser")
    urls = []
    for link in soup.find_all("a"):
        a_url = link.get("href")
        if a_url and "txt" in a_url:
            urls.append(base_url + "/" + a_url)
    return urls


if __name__ == "__main__":
    final_timeline = pd.DataFrame(columns=["Start Time", "End Time", "Instrument", "Comment"])
    for key, block in DATASETS.items():
        print(f"Scraping {key}")
        print(f"{len(final_timeline.index)} rows so far")
        urls = [block.get("URL")]
        if block.get("SCRAPE"):
            urls = scrape_url(block["URL"])
        if block.get("RANGE"):
            if block.get("MONTH_RANGE"):
                urls = [
                    block["fURL"].format(f"20{i:02}{j:02}") for i, j in product(block["RANGE"], block["MONTH_RANGE"])
                ]
            else:
                urls = [block["fURL"].format(f"20{i:02}") for i in block["RANGE"]]
        for url in sorted(urls):
            print(f"Parsing {url}")
            if "txt" in url:
                final_timeline = process_txt(url, block.get("SKIPROWS"), final_timeline)
            elif "html" in url:
                final_timeline = process_html(url, final_timeline)
            else:
                raise ValueError(f"Unknown file type for {url}")

    print(f"{len(final_timeline.index)} rows in total")
    final_timeline = final_timeline.sort_values("Start Time")
    final_timeline["End Time"] = final_timeline["End Time"].fillna("Unknown")
    final_timeline["Instrument"] = final_timeline["Instrument"].fillna("SDO")
    final_timeline["Comment"] = final_timeline["Comment"].fillna("No Comment")
    final_timeline.to_csv("timeline.csv", index=False)
    final_timeline.to_csv("timeline.txt", sep="\t", index=False)
