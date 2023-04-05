"""
Provides utilities for working with NHANES data.

Toby Rea
04-04-2023
"""

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import pyreadstat

from nhanes_utils import config
from nhanes_utils.scraper import Scraper
from nhanes_utils.downloader import Downloader


def download_nhanes(components: list[str] | None = None,
                    years: list[str] | None = None,
                    include_docs: bool = False,
                    destination: str | None = None) -> None:
    """ Downloads datasets and optionally documentation from NHANES. """

    if components is None:
        components = config.COMPONENTS
    if years is None:
        years = config.YEARS
    if destination is None:
        destination = config.DATA_DIRECTORY

    Path(destination).mkdir(parents=True, exist_ok=True)

    scraper = Scraper()
    datasets = scraper.get_datasets()

    # Filter to selection and download the datasets, including the documentation if specified
    filtered_datasets = datasets[(datasets["years"].isin(years)) & (datasets["component"].isin(components))]
    if filtered_datasets.empty:
        print("No datasets found matching the specified criteria.")
        return

    # Construct the list of urls to download
    url_list: list[str] = filtered_datasets["data_url"].tolist()
    if include_docs:
        url_list.extend(filtered_datasets["docs_url"].tolist())

    # Download all the files
    downloader = Downloader(url_list, destination)
    downloader.run()


def convert_xpt_to_csv(xpt_path: Path) -> None:
    """ Converts an XPT file to CSV, removing the original XPT file. """

    df, _ = pyreadstat.read_xport(xpt_path)
    df.to_csv(xpt_path.with_suffix(".csv"), index=False)

    # Remove the original XPT file
    os.remove(xpt_path)


def convert_datasets(data_directory: str | None = None) -> None:
    """ Converts all XPT files in the data directory to CSV. """

    if data_directory is None:
        data_directory = config.DATA_DIRECTORY

    xpt_files = [file for file in Path(data_directory).iterdir() if file.suffix.lower() == ".xpt"]
    if not xpt_files:
        print("Nothing to convert...")
        return

    print("Converting XPT files to CSV...")
    with ThreadPoolExecutor() as executor:
        executor.map(convert_xpt_to_csv, xpt_files)
    print("Conversion complete!")


def get_available_datasets() -> pd.DataFrame:
    """ Returns a pandas dataframe containing all available datasets. """

    scraper = Scraper()
    return scraper.get_datasets()
