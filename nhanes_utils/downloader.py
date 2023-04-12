import asyncio
import io
import os
from typing import List, Optional

import aiofiles
import aiohttp

from nhanes_utils.config import CHUNK_SIZE, MAX_CONCURRENT_DOWNLOADS, DOWNLOAD_RETRIES, DOWNLOAD_DIRECTORY


class Downloader:
    """
    A class for downloading files asynchronously using aiohttp and asyncio. The class takes a list of URLs and downloads
    them concurrently with a maximum number of concurrent downloads specified by the user. The downloaded files are saved
    to a specified directory, or the current working directory if none is specified. The class also includes a method for
    downloading a single file and a method for running the download process without needing an await call.
    """

    def __init__(self, url_list: List[str], download_directory: Optional[str] = DOWNLOAD_DIRECTORY) -> None:
        self.urls = url_list
        self.destination = download_directory
        self.throttler = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

        # create the destination directory if it doesn't exist
        if self.destination and not os.path.exists(self.destination):
            os.makedirs(self.destination)

    def file_exists(self, url: str) -> bool:
        return os.path.exists(os.path.join(self.destination, os.path.basename(url)))

    async def download_file(self, url: str, session: aiohttp.ClientSession) -> None:
        """
        Downloads a single file from the given URL using the specified aiohttp session. The file is downloaded in chunks
        and saved to the specified destination directory. If the download fails, the function retries up to the specified
        number of times before giving up. 
        """

        file_name = os.path.basename(url)

        if self.destination:
            file_name = os.path.join(self.destination, file_name)

        async with self.throttler:
            print(f"Downloading {url} ...")
            for i in range(DOWNLOAD_RETRIES):
                try:
                    async with session.get(url, timeout=None) as response:
                        buffer = io.BytesIO()
                        async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                            buffer.write(chunk)

                        async with aiofiles.open(file_name, 'wb') as file:
                            buffer.seek(0)
                            await file.write(buffer.read())

                        return  # exit on successful download
                except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                    if i == 2:
                        print(f"Failed to download {url} after {i + 1} retries ...")
                        return

                    print(f"Error occurred during download: {str(error)}")
                    await asyncio.sleep(5)

    async def download_files_async(self) -> None:
        """ Downloads the files from the list of URLs asynchronously. """

        urls_to_download = [url for url in self.urls if not self.file_exists(url)]

        if not urls_to_download:
            print("There is nothing to download ...")
            return

        print(f"Downloading {len(urls_to_download)} files to {self.destination}...")
        async with aiohttp.ClientSession() as session:
            tasks = [asyncio.create_task(self.download_file(url, session)) for url in urls_to_download]
            await asyncio.gather(*tasks)
            print("Downloading completed!")

    def run(self) -> None:
        """ Downloads the files from the list of URLs asynchronously without needing an await call. """

        asyncio.run(self.download_files_async())
