import config
import download_helper


def download_seasons(reset: bool = False) -> None:
    """
    Download the seasons json (there is only one file to download)
    :param reset: Whether to re-download the file if it already exists
    :return: State of local machine should now have copy of file
    """
    if type(reset) != bool:
        raise ValueError(f"Type of reset <{type(reset)}> should be bool!")
    download_helper.mkdir(config.DIR_JSON)
    download_helper.downloader(config.URL_SEASONS, config.FILE_SEASONS, reset=reset)


def reset_seasons_all() -> None:
    """
    Since there is only one file, this will re-download the one file to make sure it is recent
    :return: State of local machine should now have copy of file (most recent)
    """
    download_seasons(True)


def main() -> None:
    """
    Used to test this file
    :return: None
    """
    reset_seasons_all()


if __name__ == '__main__':
    main()
    print("Done")
