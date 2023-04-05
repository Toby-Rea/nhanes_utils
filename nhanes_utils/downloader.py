"""
Downloads files from a list of urls asynchronously.

Toby Rea
05-04-2023
"""

import asyncio
from pathlib import Path

import aiohttp


class Downloader:
    def __init__(self, url_list: list[str], destination: str):
        self.url_list: list[str] = url_list
        self.destination: str = destination

    async def download_file(self, url: str) -> None:
        """ Downloads a file from a given url, if it doesn't already exist. """

        file_name = url.split("/")[-1].lower()
        xpt_file = Path(self.destination) / file_name
        csv_file = Path(self.destination) / file_name.replace(".xpt", ".csv")
        if xpt_file.exists() or csv_file.exists():
            print(f"{file_name} exists already!")
            return

        async with aiohttp.ClientSession() as session, session.get(url) as response:
            if response.status == 200:
                content = await response.read()
                with open(xpt_file, "wb") as file:
                    file.write(content)
            else:
                print(f"Failed to download from {url}")

    async def download(self) -> None:
        """ Downloads all files stored in the url list. """

        print(f"Downloading files to {self.destination}...")
        tasks = []
        for url in self.url_list:
            task = asyncio.create_task(self.download_file(url))
            tasks.append(task)

        await asyncio.gather(*tasks)

        print("Downloading complete!")

    def run(self) -> None:
        """ Runs the downloader. """

        asyncio.run(self.download())
