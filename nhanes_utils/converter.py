import asyncio
import os
from typing import List

import pandas as pd


class Converter:
    """
    A class for converting XPT files to CSV format asynchronously. The original XPT files are removed after conversion. 
    """

    def __init__(self, xpt_files: List[str]):
        self.xpt_files = xpt_files

    async def convert_xpt_to_csv_async(self, xpt_path: str) -> None:
        """ Converts an XPT file to CSV, removing the original XPT file. """

        print(f"Converting {xpt_path} to CSV ...")
        df = pd.read_sas(xpt_path)

        csv_path = os.path.splitext(xpt_path)[0] + ".csv"
        df.to_csv(csv_path, index=False)

        # Remove the original XPT file
        os.remove(xpt_path)

    async def convert(self) -> None:
        """ Converts all XPT files in the list of XPT files asynchronously. """

        tasks = [self.convert_xpt_to_csv_async(xpt_file) for xpt_file in self.xpt_files]
        await asyncio.gather(*tasks)
        print("Conversion complete!")

        print("Converting XPT files to CSV ...")
        tasks = [self.convert_xpt_to_csv_async(xpt_file) for xpt_file in self.xpt_files]
        await asyncio.gather(*tasks)
        print("Conversion complete!")

    def run(self) -> None:
        """ Converts the files from the list of XPT files asynchronously without needing an await call. """

        asyncio.run(self.convert())
