import os
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import pandas as pd
import polars as pl

from nhanes_utils.config import COMPONENTS, YEARS, DOWNLOAD_DIRECTORY
from nhanes_utils.downloader import Downloader
from nhanes_utils.scraper import Scraper


def download_nhanes(components: Optional[list[str]] = None, years: Optional[list[str]] = None, incl_docs: bool = False):
    """ Downloads datasets and optionally documentation from NHANES. """

    if components is None:
        components = COMPONENTS
    if years is None:
        years = YEARS

    scraper = Scraper()
    datasets = scraper.get_datasets()

    for component in components:
        df = datasets.filter((pl.col("years").is_in(years)) & (pl.col("component") == component))
        url_list: list[str] = df["data_url"].to_list()
        if incl_docs:
            url_list.extend(df["docs_url"].to_list())

        downloader = Downloader(url_list, f"{DOWNLOAD_DIRECTORY}/{component.lower()}")
        downloader.run()


def convert_xpt_to_csv(xpt_path: str) -> None:
    """ Converts an XPT file to CSV, removing the original XPT file. """

    print(f"Converting {xpt_path} to CSV...")
    df = pd.read_sas(xpt_path)

    csv_path = os.path.splitext(xpt_path)[0] + ".csv"
    df.to_csv(csv_path, index=False)

    # Remove the original XPT file
    os.remove(xpt_path)


def convert_datasets() -> None:
    """ Converts all XPT files in the data directory to CSV. """

    # Use os to get all xpt files in the data directory
    xpt_files = []
    for dp, dn, filenames in os.walk(DOWNLOAD_DIRECTORY):
        for f in filenames:
            if os.path.splitext(f)[1].lower() == ".xpt":
                xpt_files.append(os.path.join(dp, f))

    if not xpt_files:
        print("Nothing to convert...")
        return

    print("Converting XPT files to CSV...")
    with ThreadPoolExecutor() as executor:
        executor.map(convert_xpt_to_csv, xpt_files)
    print("Conversion complete!")


def get_available_datasets() -> pl.DataFrame:
    """ Returns a pandas dataframe containing all available datasets. """

    scraper = Scraper()
    return scraper.get_datasets()
