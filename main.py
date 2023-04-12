from nhanes_utils import download_nhanes, convert_datasets


def main() -> None:
    # Default behaviour is to download all known components over all known years, excluding documentation.
    #
    # You can override this behaviour by passing specific options here...
    #download_nhanes()

    # Optionally, convert all the XPT files to CSV, a human-readable format
    convert_datasets()


if __name__ == "__main__":
    main()
