"""
Provides utilities for scraping the NHANES website for available datasets.

Toby Rea
04-04-2023
"""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import requests
from selectolax.parser import HTMLParser

from nhanes_utils import config
from nhanes_utils.dataset import Dataset


class Scraper:
    def __init__(self):
        self.datasets = pd.DataFrame(columns=["years", "component", "description", "data_url", "docs_url"])

    def get_datasets(self) -> pd.DataFrame:
        """ Returns a dataframe of all publicly available NHANES datasets. """

        if not Path("nhanes_datasets.csv").is_file():
            print("Available datasets unknown...")
            return self.scrape_datasets()

        return pd.read_csv("nhanes_datasets.csv")

    def scrape_datasets(self) -> pd.DataFrame:
        """ Scrapes all publicly available NHANES datasets. """

        print("Scraping NHANES for available datasets...")
        with ThreadPoolExecutor() as executor:
            executor.map(self.scrape_component, config.COMPONENTS)

        # Write the dataframe to a csv file for future use
        self.datasets.to_csv("nhanes_datasets.csv", index=False)
        print("Scraping complete!")

        return self.datasets

    def scrape_component(self, component: str) -> None:
        """ Scrapes the available datasets for a given component. """

        base = "https://wwwn.cdc.gov"
        new_url = config.URL + component
        response = requests.get(new_url, headers=config.HEADERS)
        tree = HTMLParser(response.text)
        selector = "table > tbody > tr"

        datasets: list[Dataset] = []
        for node in tree.css(selector):
            if "limited_access" in node.html.lower():
                continue
            if "withdrawn" in node.html.lower():
                continue
            if node.css_first("td:nth-child(4) > a") is None:
                continue

            years = node.css_first("td:nth-child(1)").text().strip()
            description = node.css_first("td:nth-child(2)").text().strip()
            docs_url = base + node.css_first("td:nth-child(3) > a") \
                .attributes["href"].strip()
            data_url = base + node.css_first("td:nth-child(4) > a") \
                .attributes["href"].strip()

            if not data_url.lower().endswith(".xpt"):
                continue

            dataset = Dataset(years, component, description, data_url, docs_url)
            datasets.append(dataset)

        # Add these datasets to the dataframe
        df = pd.DataFrame([dataset.__dict__ for dataset in datasets])
        self.datasets = pd.concat([self.datasets, df], ignore_index=True)
