import json
import os

import config
import download_helper


def download_players_year_range(start: int, end: int, reset: bool = False, update: bool = False) -> None:
    """
    Download the players json
    :param start: The first year
    :param end: The last year
    :param reset: Whether to re-download the file if it already exists
    :param update: Whether to check only if there is last page or more has changed
    :return: State of local machine should now have copy of the files
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    if type(reset) != bool:
        raise ValueError(f"Type of reset <{type(reset)}> should be bool!")
    if type(update) != bool:
        raise ValueError(f"Type of update <{type(update)}> should be bool!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not downloading any players as nothing in years=[{start},{end}]!")
    for year in sorted(years):
        download_players_year(year=year, reset=reset, update=update)


def download_players_year(year: int = None, reset: bool = False, update: bool = False) -> None:
    """
    Download the players json
    :param year: The year
    :param reset: Whether to re-download the file if it already exists
    :param update: Whether to check only if there is last page or more has changed
    :return: State of local machine should now have copy of the file
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    if type(reset) != bool:
        raise ValueError(f"Type of reset <{type(reset)}> should be bool!")
    if type(update) != bool:
        raise ValueError(f"Type of update <{type(update)}> should be bool!")
    download_helper.mkdir(config.DIR_JSON)
    download_helper.mkdir(config.DIR_PLAYERS)
    if year:
        dir_year = config.DIR_PLAYERS.joinpath(str(year))
    else:
        dir_year = config.DIR_PLAYERS.joinpath("null")
    download_helper.mkdir(dir_year)
    page = 1
    # If update only we begin at last page and start with reset mode
    if update:
        page = max([int(x.replace(".json", "")) for x in os.listdir(dir_year)])
        reset = True
    # We need parameters due to paging and ordering of the shear number of players
    parameters = {}
    # At most can get 100 at time
    parameters['page[size]'] = "100"
    # Sort by id for consistency (wish it was clear ids were sequentially added over time but not clear this is true)
    parameters['sort'] = "cfl_central_id"
    # To not re-download all we'll download by rook_year, or by unassigned rookie_year
    if year:
        parameters['filter[rookie_year][eq]'] = str(year)
    else:
        parameters['filter[rookie_year][lt]'] = 1900
    while True:
        parameters['page[number]'] = str(page)
        file_players = dir_year.joinpath(str(page) + ".json")
        download_helper.downloader(config.URL_PLAYERS, file_players, parameters=parameters, reset=reset)
        # We will stop if the last file had less than a full page of players
        with open(file_players) as file:
            text = file.read()
            json_string = json.loads(text)
            data = json_string['data']
            if not data or len(data) < 100:
                return
        page += 1


def download_players_null_rookie_year(reset: bool = False, update: bool = False) -> None:
    """
    Download the players json for rookie_year is null
    :param reset: Whether to re-download the file if it already exists
    :param update: Whether to check only if there is last page or more has changed
    :return: State of local machine should now have copy of the file
    """
    download_players_year(year=None, reset=reset, update=update)


def reset_players_all() -> None:
    """
    Since there is more than one file this will re-download multiple
    :return: State of local machine should now have copy of files (most recent)
    """
    download_players_year_range(config.YEAR_PLAYERS_MIN_ROOKIE_YEAR, config.YEAR_PLAYERS_MAX_ROOKIE_YEAR, reset=True,
                                update=False)
    download_players_null_rookie_year(reset=True, update=False)


def reset_players_year(year: int) -> None:
    """
    Since there is only one file, this will re-download the one file to make sure it is recent
    :param year: The year
    :return: State of local machine should now have copy of file (most recent)
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    download_players_year(year, reset=True, update=False)
    download_players_null_rookie_year(reset=False, update=True)


def reset_players_year_current() -> None:
    """
    Since there is only one file, this will re-download the one file to make sure it is recent
    :return: State of local machine should now have copy of file (most recent)
    """
    reset_players_year(config.YEAR_CURRENT)


def main() -> None:
    """
    Used to test this file
    :return: None
    """
    # Reset players for all years
    if False:
        reset_players_all()
    # Reset players for a specific year
    if False:
        year = 2022
        reset_players_year(year)
    # Reset players for current year
    if True:
        reset_players_year_current()


if __name__ == '__main__':
    main()
    print("Done")
