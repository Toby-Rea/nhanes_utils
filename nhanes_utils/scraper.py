import os
from concurrent.futures import ThreadPoolExecutor
from typing import List

import polars as pl
import requests
from selectolax.lexbor import LexborHTMLParser

from nhanes_utils.config import COMPONENTS, SCRAPER_BASE_URL, DATASETS_CSV
from nhanes_utils.dataset import Dataset


class Scraper:
    """
    A class for scraping NHANES datasets from the CDC website. Provides methods for scraping
    datasets for each component and getting all datasets. Datasets are parsed into a DataFrame using
    Polars and can be saved to a local file for future use.
    """

    def __init__(self):
        self.datasets: List[Dataset] = list()

    def parse_row(self, td_list: List, component: str) -> None:
        """ Parse a dataset from the row, adding it the list of datasets. """

        base_url = "https://wwwn.cdc.gov"
        docs_anchor = td_list[2].css_first("a")
        data_anchor = td_list[3].css_first("a")

        # Ensure the dataset has both a link to the data and documentation
        if not (docs_anchor and data_anchor):
            return

        # Ensure we only scrape datasets with data in the XPT format
        if not data_anchor.attrs['href'].strip().lower().endswith(".xpt"):
            return

        dataset = Dataset(years=td_list[0].text().strip(),
                          component=component,
                          description=td_list[1].text().strip(),
                          docs_url=f"{base_url}{docs_anchor.attrs['href'].strip()}",
                          data_url=f"{base_url}{data_anchor.attrs['href'].strip()}")

        self.datasets.append(dataset)

    def scrape_component(self, component: str) -> None:
        """ Scrape NHANES for a specific component. """

        print(f"Scraping NHANES for {component} datasets ...")

        response = requests.get(f"{SCRAPER_BASE_URL}{component}")
        parser = LexborHTMLParser(response.text)
        for row in parser.css_first("table > tbody").css("tr"):
            self.parse_row(row.css("td"), component)

    def scrape(self) -> pl.DataFrame:
        """ Scrape NHANES datasets for each of the components. """

        with ThreadPoolExecutor() as executor:
            executor.map(self.scrape_component, COMPONENTS)

        print(f"Scraped {len(self.datasets)} datasets!")
        return pl.DataFrame(self.datasets)

    def get_datasets(self, fresh: bool = False) -> pl.DataFrame:
        """ Get NHANES datasets. If fresh is True, scrape the website. Otherwise, read from CSV if it exists. """

        if os.path.isfile(DATASETS_CSV) and not fresh:
            return pl.read_csv(DATASETS_CSV)

        df = self.scrape()
        df.write_csv(DATASETS_CSV)
        return df
