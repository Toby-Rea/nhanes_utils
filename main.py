"""
Provides utilities for working with NHANES data.

TODO: Support appending data
TODO: Support merging data

Toby Rea
01-01-2023
"""


import requests
from selectolax.parser import HTMLParser
import pandas as pd
from pathlib import Path


URL = "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component="
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


def scrape_datasets() -> pd.DataFrame:
    """ Scrapes all publicly available NHANES datasets. """

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"}

    df = pd.DataFrame(columns=["years", "component", "description", "docs_url", "data_url"])
    base = "https://wwwn.cdc.gov"
    for component in COMPONENTS:
        print(f"[scraping] {component}")
        new_url = URL + component
        response = requests.get(new_url, headers=headers)
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
            docs_url = base + node.css_first("td:nth-child(3) > a").attributes["href"].strip()
            data_url = base + node.css_first("td:nth-child(4) > a").attributes["href"].strip()

            if not data_url.lower().endswith(".xpt"):
                continue

            df.loc[len(df.index)] = [years, component, description, docs_url, data_url]

    return df


def download(url: str) -> None:
    """ Downloads a file from a url, if it doesn't already exist. """

    file_name = url.split("/")[-1]
    file_path = Path(DATA_DIRECTORY) / file_name
    if file_path.exists():
        print(f"[skipping] {file_name} - file already exists")
        return

    print(f"[downloading] {file_name} from \"{url}\"")
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

    # Create the available nhanes dataset if it doesn't exist
    if not Path("nhanes_datasets.csv").is_file():
        print("Available datasets unknown - Scraping NHANES...")
        df = scrape_datasets()
        df.to_csv("nhanes_datasets.csv", index=False)

    df = pd.read_csv("nhanes_datasets.csv")

    # Filter and download
    df = df[(df["years"].isin(years)) & (df["component"].isin(components))]
    [download(url) for url in df["data_url"]]
    [download(url) for url in df["docs_url"] if include_docs]


def main() -> None:
    # Default behaviour is to download all known components over all known
    # years, excluding documentation.
    #
    # You can override this behaviour by passing options here
    download_nhanes(
        components=["Laboratory"],
        years=["2013-2014"],
        include_docs=True
    )


if __name__ == "__main__":
    main()
