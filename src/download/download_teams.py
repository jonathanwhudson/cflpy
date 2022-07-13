import config
import download_helper


def download_teams(reset: bool = False) -> None:
    """
    Download the teams json (there is only one file to download)
    :param reset: Whether to re-download the file if it already exists
    :return: State of local machine should now have copy of file
    """
    if type(reset) != bool:
        raise ValueError(f"Type of reset <{type(reset)}> should be bool!")
    download_helper.mkdir(config.DIR_JSON)
    download_helper.downloader(config.URL_TEAMS, config.FILE_TEAMS, reset=reset)


def reset_teams_all() -> None:
    """
    Since there is only one file, this will re-download the one file to make sure it is recent
    :return: State of local machine should now have copy of file (most recent)
    """
    download_teams(True)


def main() -> None:
    """
    Used to test this file
    :return: None
    """
    reset_teams_all()


if __name__ == '__main__':
    main()
    print("Done")
