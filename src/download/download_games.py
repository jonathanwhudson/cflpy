from urllib.parse import urljoin

import config
import download_helper


def download_games_year_range(start: int, end: int, reset: bool = False) -> set[int]:
    """
    Download the games json
    :param start: The first year
    :param end: The last year
    :param reset: Whether to re-download the files if they already exist
    :return: State of local machine should now have copy of the files
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    if type(reset) != bool:
        raise ValueError(f"Type of reset <{type(reset)}> should be bool!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not downloading any games as nothing in years=[{start},{end}]!")
    download_helper.mkdir(config.DIR_JSON)
    download_helper.mkdir(config.DIR_GAMES)
    downloaded = set()
    for year in sorted(years):
        downloaded.update(download_games_year(year, reset=reset))
    return downloaded


def download_games_year(year: int, reset: bool = False) -> set[int]:
    """
    Download the games json
    :param year: The year
    :param reset: Whether to re-download the file if it already exists
    :return: State of local machine should now have copy of the files
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    if type(reset) != bool:
        raise ValueError(f"Type of reset <{type(reset)}> should be bool!")
    url_year = urljoin(config.URL_GAMES, str(year))
    dir_basic = config.DIR_GAMES.joinpath("basic")
    download_helper.mkdir(dir_basic)
    filename_year = dir_basic.joinpath(f"{year}.json")
    downloaded = set()
    if download_helper.downloader(url_year, filename_year, reset=reset):
        downloaded.add(year)
    return downloaded


def reset_games_all() -> None:
    """
    Since there is more than one file this will re-download multiple
    :return: State of local machine should now have copy of files (most recent)
    """
    download_games_year_range(config.YEAR_START_GAMES, config.YEAR_END_GAMES, reset=True)


def reset_games_year(year: int) -> None:
    """
    Since there is only one file, this will re-download the one file to make sure it is recent
    :param year: The year
    :return: State of local machine should now have copy of file (most recent)
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    download_games_year(year, reset=True)


def reset_games_year_current() -> None:
    """
    Since there is only one file, this will re-download the one file to make sure it is recent
    :return: State of local machine should now have copy of file (most recent)
    """
    reset_games_year(config.YEAR_CURRENT)


def main() -> None:
    """
    Used to test this file
    :return: None
    """
    # Reset games for all years
    if False:
        reset_games_all()
    # Reset games for a specific year
    if False:
        year = 2022
        reset_games_year(year)
    # Reset games for current year
    if True:
        reset_games_year_current()


if __name__ == '__main__':
    main()
    print("Done")
