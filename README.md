# nhanes-utils

Provides a set of useful utilities for accessing and manipulating NHANES data.

Example usage:

```python
def main() -> None:
    # Leave blank for all known components over all known years
    # documentation is excluded by default
    download_nhanes()

    # Provide parameters to be more specific
    download_nhanes(components=["Demographics"], years=["2013-2014"], include_docs=True)```
