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

        # Get the file name and extension from the url, and convert the file extension to lowercase.
        file_name = url.split("/")[-1]
        extension = file_name.split(".")[-1]
        file_name = file_name.replace(extension, extension.lower())

        # Ensure the file doesn't already exist.
        exists: bool = False
        path = Path(self.destination).joinpath(file_name)
        match extension.lower():
            case "xpt" | "csv":
                exists = path.with_suffix(".xpt").exists() or path.with_suffix(".csv").exists()
            case _:
                exists = path.exists()

        if exists:
            return

        async with aiohttp.ClientSession() as session, session.get(url) as response:
            if response.status == 200:
                content = await response.read()
                with open(path, "wb") as file:
                    file.write(content)
            else:
                print(f"Failed to download from {url} (received response code {response.status}).")

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
