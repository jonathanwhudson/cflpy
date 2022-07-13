import config
import download_helper


def download_venues(reset: bool = False) -> None:
    """
    Download the venues json (there is only one file to download)
    :param reset: Whether to re-download the file if it already exists
    :return: State of local machine should now have copy of file
    """
    if type(reset) != bool:
        raise ValueError(f"Type of reset <{type(reset)}> should be bool!")
    download_helper.mkdir(config.DIR_JSON)
    download_helper.downloader(config.URL_VENUES, config.FILE_VENUES, reset=reset)


def reset_venues_all() -> None:
    """
    Since there is only one file, this will re-download the one file to make sure it is recent
    :return: State of local machine should now have copy of file (most recent)
    """
    download_venues(True)


def main() -> None:
    """
    Used to test this file
    :return: None
    """
    reset_venues_all()


if __name__ == '__main__':
    main()
    print("Done")
