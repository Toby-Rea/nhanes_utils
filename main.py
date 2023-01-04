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
import tomllib
from tqdm import tqdm

def scrape_datasets() -> pd.DataFrame:
    """ Scrapes all publicly available NHANES datasets. """

    with open("settings.toml", "rb") as config_file:
        config = tomllib.load(config_file)
        components = config["components"]
        url = config["url"]

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"}

    df = pd.DataFrame(columns=["years", "component", "docs_url", "data_url"])
    base = "https://wwwn.cdc.gov"
    for component in components:
        new_url = url + component
        response = requests.get(new_url, headers=headers)
        tree = HTMLParser(response.text)
        selector = "table > tbody > tr"

        for node in tqdm(tree.css(selector), bar_format="{desc:25}|{bar:25}{r_bar}", desc=f"Component: {component}"):
            if "limited_access" in node.html.lower():
                continue
            if "withdrawn" in node.html.lower():
                continue
            if node.css_first("td:nth-child(4) > a") is None:
                continue

            years = node.css_first("td:nth-child(1)").text().strip()
            docs_url = base + node.css_first("td:nth-child(3) > a").attributes["href"].strip()
            data_url = base + node.css_first("td:nth-child(4) > a").attributes["href"].strip()

            if not data_url.lower().endswith(".xpt"):
                continue

            df.loc[len(df.index)] = [years, component, docs_url, data_url]

    return df


def download_file(url: str, destination: Path) -> None:
    """ Downloads a file from a url, if it doesn't already exist. """

    file_name = url.split("/")[-1]
    file_path = destination / file_name
    if file_path.exists():
        print(f"Skipping {file_name} ... File already exists.")
        return

    print(f"Downloading {file_name} ...")
    response = requests.get(url)
    with open(file_path, "wb") as file:
        file.write(response.content)


def download_nhanes(components: list[str] | None = None,
                    years: list[str] | None = None,
                    include_docs: bool = False) -> None:
    """ Downloads datasets and optionally documentation from NHANES. """

    with open("settings.toml", "rb") as config:
        destination = Path(tomllib.load(config)["data_directory"])
        if not destination.is_dir():
            Path.mkdir(destination)

    if components is None:
        with open("settings.toml", "rb") as config:
            components = tomllib.load(config)["components"]
    if years is None:
        with open("settings.toml", "rb") as config:
            years = tomllib.load(config)["years"]

    # Create the available nhanes dataset if it doesn't exist
    if not Path("nhanes_datasets.csv").is_file():
        print("Available datasets unknown - Scraping NHANES...")
        df = scrape_datasets()
        df.to_csv("nhanes_datasets.csv", index=False)

    df = pd.read_csv("nhanes_datasets.csv")

    # Filter and download
    df = df[(df["years"].isin(years)) & (df["component"].isin(components))]
    [download_file(data, destination) for data in df["data_url"]]
    if include_docs:
        [download_file(doc, destination) for doc in df["docs_url"]]


def main() -> None:
    # Default behaviour is to download all components over all years
    # excluding documentation. You can override this behaviour by
    # passing options here or overriding 'settings.toml'
    download_nhanes(
        components=["Laboratory"],
        years=["2013-2014"],
        include_docs=True
    )


if __name__ == "__main__":
    main()
