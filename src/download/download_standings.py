from urllib.parse import urljoin

import config
import download_helper


def download_standings_year_range(start: int, end: int, reset: bool = False) -> None:
    """
    Download the standings json (there is one file per year, and once crossover starts, one file per year additional)
    :param start: The first year
    :param end: The last year
    :param reset: Whether to re-download the file if it already exists
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
        raise ValueError(f"Not downloading any standings as nothing in years=[{start},{end}]!")
    for year in sorted(years):
        download_standings_year(year, reset=reset)


def download_standings_year(year: int, reset: bool = False) -> None:
    """
    Download the standings json (there is one file per year, and once crossover starts, one file per year additional)
    :param year: The year
    :param reset: Whether to re-download the file if it already exists
    :return: State of local machine should now have copy of the files
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    if type(reset) != bool:
        raise ValueError(f"Type of reset <{type(reset)}> should be bool!")
    download_helper.mkdir(config.DIR_JSON)
    download_helper.mkdir(config.DIR_STANDINGS)
    download_helper.mkdir(config.DIR_CROSSOVER)
    file_standings = config.DIR_STANDINGS.joinpath(str(year) + ".json")
    file_crossover = config.DIR_CROSSOVER.joinpath(str(year) + ".json")
    url_standings = urljoin(config.URL_STANDINGS, str(year) + "/")
    url_crossover = urljoin(config.URL_CROSSOVER, str(year) + "/")
    download_helper.downloader(url_standings, file_standings, reset=reset)
    if year >= config.YEAR_CROSSOVER_STARTS:
        download_helper.downloader(url_crossover, file_crossover, reset=reset)


def reset_standings_all() -> None:
    """
    Since there is more than one file this will re-download multiple
    :return: State of local machine should now have copy of files (most recent)
    """
    download_standings_year_range(config.YEAR_START_GAMES, config.YEAR_END_GAMES, reset=True)


def reset_standings_year(year: int) -> None:
    """
    Since there is only one file, this will re-download the one file to make sure it is recent
    :param year: The year
    :return: State of local machine should now have copy of file (most recent)
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    download_standings_year(year, reset=True)


def reset_standings_year_current() -> None:
    """
    Since there is only one file, this will re-download the one file to make sure it is recent
    :return: State of local machine should now have copy of file (most recent)
    """
    reset_standings_year(config.YEAR_CURRENT)


def main() -> None:
    """
    Used to test this file
    :return: None
    """
    # Reset standings for all years
    if False:
        reset_standings_all()
    # Reset standings for a specific year
    if False:
        year = 2022
        reset_standings_year(year)
    # Reset standings for current year
    if True:
        reset_standings_year_current()


if __name__ == '__main__':
    main()
    print("Done")
