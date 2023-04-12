import os
from typing import Optional

import polars as pl

from nhanes_utils.config import COMPONENTS, YEARS, DOWNLOAD_DIRECTORY
from nhanes_utils.converter import Converter
from nhanes_utils.downloader import Downloader
from nhanes_utils.scraper import Scraper


def download_nhanes(components: Optional[list[str]] = None, years: Optional[list[str]] = None, incl_docs: bool = False):
    """ Downloads datasets and optionally documentation from NHANES. """

    if components is None:
        components = COMPONENTS
    if years is None:
        years = YEARS

    datasets = get_available_datasets()

    for component in components:
        df = datasets.filter((pl.col("years").is_in(years)) & (pl.col("component") == component))
        url_list: list[str] = df["data_url"].to_list()
        if incl_docs:
            url_list.extend(df["docs_url"].to_list())

        downloader = Downloader(url_list, f"{DOWNLOAD_DIRECTORY}/{component.lower()}")
        downloader.run()


def convert_datasets() -> None:
    """ Converts all XPT files in the data directory to CSV. """

    # Store references to all XPT files in the data directory
    xpt_files = []
    for root, _, files in os.walk(DOWNLOAD_DIRECTORY):
        for file in files:
            if os.path.splitext(file)[1].lower() == ".xpt":
                xpt_files.append(os.path.join(root, file))

    if not xpt_files:
        print("Nothing to convert ...")
        return

    converter = Converter(xpt_files)
    converter.run()


def get_available_datasets() -> pl.DataFrame:
    """ Returns a pandas dataframe containing all available datasets. """

    scraper = Scraper()
    return scraper.get_datasets()
