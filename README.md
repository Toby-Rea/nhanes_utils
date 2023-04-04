# nhanes_utils

Provides a set of useful utilities for accessing and manipulating NHANES data.

## Example Usage

```python
from nhanes_utils import download_nhanes, convert_datasets


def main() -> None:
    # Default behaviour is to download all known components over all known years, excluding documentation.
    #
    # You can override this behaviour by passing specific options here. For example, to download only the Laboratory
    # datasets for 2011-2012, including documentation:
    download_nhanes(components=["Laboratory"], years=["2011-2012"], include_docs=True)

    # Optionally, convert all the XPT files to CSV, a human-readable format
    convert_datasets()


if __name__ == "__main__":
    main()
```