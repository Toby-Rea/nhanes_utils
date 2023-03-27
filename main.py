"""
Provides utilities for working with NHANES data.

TODO: Support appending data
TODO: Support merging data

Toby Rea
01-01-2023
"""

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import pyreadstat
import requests
from selectolax.parser import HTMLParser

URL = "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component="
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"}
DATA_DIRECTORY = "data"
COMPONENTS = [
    "Demographics",
    "Dietary",
    "Examination",
    "Laboratory",
    "Questionnaire"
]
YEARS = [
    "1999-2000",
    "2001-2002",
    "2003-2004",
    "2005-2006",
    "2007-2008",
    "2009-2010",
    "2011-2012",
    "2013-2014",
    "2015-2016",
    "2017-2018",
    "2017-2020",
    "2019-2020",
    "2021-2022"
]


class Scraper:
    def __init__(self):
        self.datasets = pd.DataFrame(columns=["years", "component", "description", "docs_url", "data_url"])

    def get_datasets(self) -> pd.DataFrame:
        """ Returns a dataframe of all publicly available NHANES datasets. """

        if not Path("nhanes_datasets.csv").is_file():
            print("Available datasets unknown - Scraping NHANES...")
            return self.scrape_datasets()

        return pd.read_csv("nhanes_datasets.csv")

    def scrape_datasets(self) -> pd.DataFrame:
        """ Scrapes all publicly available NHANES datasets. """

        with ThreadPoolExecutor() as executor:
            executor.map(self.scrape_component, COMPONENTS)

        # Write the dataframe to a csv file for future use
        self.datasets.to_csv("nhanes_datasets.csv", index=False)

        return self.datasets

    def scrape_component(self, component: str) -> None:
        """ Scrapes the available datasets for a given component. """

        print(f"Scraping {component} datasets...")
        base = "https://wwwn.cdc.gov"
        new_url = URL + component
        response = requests.get(new_url, headers=HEADERS)
        tree = HTMLParser(response.text)
        selector = "table > tbody > tr"

        for node in tree.css(selector):
            if "limited_access" in node.html.lower():
                continue
            if "withdrawn" in node.html.lower():
                continue
            if node.css_first("td:nth-child(4) > a") is None:
                continue

            years = node.css_first("td:nth-child(1)").text().strip()
            description = node.css_first("td:nth-child(2)").text().strip()
            doc_url = base + node.css_first("td:nth-child(3) > a") \
                .attributes["href"].strip()
            data_url = base + node.css_first("td:nth-child(4) > a") \
                .attributes["href"].strip()

            if not data_url.lower().endswith(".xpt"):
                continue

            self.datasets.loc[len(self.datasets)] = [years, component, description, doc_url, data_url]


def download(url: str) -> None:
    """ Downloads a file from a given url, if it doesn't already exist. """

    file_name = url.split("/")[-1]
    file_path = Path(DATA_DIRECTORY) / file_name
    if file_path.exists():
        return

    response = requests.get(url)
    with open(file_path, "wb") as file:
        file.write(response.content)


def download_nhanes(components: list[str] | None = None,
                    years: list[str] | None = None,
                    include_docs: bool = False,
                    destination: str | None = None) -> None:
    """ Downloads datasets and optionally documentation from NHANES. """

    if components is None:
        components = COMPONENTS
    if years is None:
        years = YEARS
    if destination is None:
        destination = DATA_DIRECTORY

    Path(destination).mkdir(parents=True, exist_ok=True)

    scraper = Scraper()
    datasets = scraper.get_datasets()

    # Filter to selection and download the datasets, and the documentation if specified
    filtered_datasets = datasets[(datasets["years"].isin(years)) & (datasets["component"].isin(components))]
    with ThreadPoolExecutor() as executor:
        print("Downloading data files...")
        executor.map(download, filtered_datasets["data_url"])
        if include_docs:
            print("Downloading documentation files...")
            executor.map(download, filtered_datasets["docs_url"])

    print("Downloading complete!")


def convert_xpt_to_csv(xpt_path: Path) -> None:
    """ Converts an XPT file to CSV, removing the original XPT file. """

    df, _ = pyreadstat.read_xport(xpt_path)
    df.to_csv(xpt_path.with_suffix(".csv"), index=False)

    # Remove the original XPT file
    os.remove(xpt_path)


def convert_datasets() -> None:
    """ Converts all XPT files in the data directory to CSV. """

    print("Converting XPT files to CSV...")
    xpt_files = [file for file in Path(DATA_DIRECTORY).iterdir() if file.suffix.lower() == ".xpt"]
    with ThreadPoolExecutor() as executor:
        executor.map(convert_xpt_to_csv, xpt_files)


def main() -> None:
    # Default behaviour is to download all known components over all known
    # years, excluding documentation.
    #
    # You can override this behaviour by passing specific options here
    # For example, to download only the Laboratory datasets for 2017-2018, including documentation:
    download_nhanes(
        components=["Laboratory"],
        years=["2017-2018"],
        include_docs=True
    )

    # Optionally, convert all the XPT files to CSV, a human-readable format
    convert_datasets()


if __name__ == "__main__":
    main()
