"""
Provides utilities for working with NHANES data.

Toby Rea
04-04-2023
"""

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyreadstat
import requests

from nhanes_utils import config
from nhanes_utils.scraper import Scraper


def download(url: str, destination: str | None = None) -> None:
    """ Downloads a file from a given url, if it doesn't already exist. """

    if destination is None:
        destination = config.DATA_DIRECTORY

    file_name = url.split("/")[-1].lower()
    xpt_file = Path(destination) / file_name
    csv_file = Path(destination) / file_name.replace(".xpt", ".csv")
    if xpt_file.exists() or csv_file.exists():
        return

    response = requests.get(url)
    with open(xpt_file, "wb") as file:
        file.write(response.content)


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

    print("Downloading datasets...")
    with ThreadPoolExecutor() as executor:
        executor.map(download, filtered_datasets["data_url"], [destination]*len(filtered_datasets["data_url"]))

        if include_docs:
            print("Downloading documentation files...")
            executor.map(download, filtered_datasets["docs_url"], [destination]*len(filtered_datasets["data_url"]))

    print("Downloading complete!")


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
