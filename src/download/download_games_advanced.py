import os
from urllib.parse import urljoin

import config
import download_helper
from store import store_games


def download_advanced_year_range(start: int, end: int, reset: bool = False) -> set[(int, int)]:
    """
    Download the advanced games json
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
        raise ValueError(f"Not downloading any advanced games as nothing in years=[{start},{end}]!")
    download_helper.mkdir(config.DIR_JSON)
    download_helper.mkdir(config.DIR_GAMES)
    downloaded = set()
    for year in sorted(years):
        downloaded.update(download_advanced_year(year, reset=reset))
    return downloaded


def download_advanced_year(year: int, reset: bool = False) -> set[(int, int)]:
    """
    Download the advanced games json
    :param year: The year
    :param reset: Whether to re-download the files if they already exist
    :return: State of local machine should now have copy of the files
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    if type(reset) != bool:
        raise ValueError(f"Type of reset <{type(reset)}> should be bool!")
    games = store_games.extract_games_year(year)
    return download_advanced_games(games, reset=reset)


def download_advanced_games(games: set[tuple[int, int]], reset: bool = False) -> set[(int, int)]:
    """
    Download the advanced games json
    :param games: Which (year, game_id) to download (instead of them all)
    :param reset: Whether to re-download the files if they already exist
    :return: State of local machine should now have copy of the files
    """
    if type(games) != set:
        raise ValueError(f"Type of limit <{type(games)}> should be set!")
    if type(reset) != bool:
        raise ValueError(f"Type of reset <{type(reset)}> should be bool!")
    downloaded = set()
    for game in games:
        downloaded.update(download_advanced_game(game, reset=reset))
    return downloaded


def download_advanced_game(game: tuple[int, int], reset: bool = False) -> set[(int, int)]:
    """
    Download the advanced games json
    :param game: (year, game_id) of game
    :param reset: Whether to re-download the files if they already exist
    :return: State of local machine should now have copy of the files
    """
    if type(game) != tuple:
        raise ValueError(f"Type of game <{type(game)}> should be tuple!")
    if type(reset) != bool:
        raise ValueError(f"Type of reset <{type(reset)}> should be bool!")
    year, game_id = game
    dir_advanced: os.path = config.DIR_GAMES.joinpath("advanced")
    download_helper.mkdir(dir_advanced)
    dir_year = dir_advanced.joinpath(str(year))
    download_helper.mkdir(dir_year)
    filename_game: os.path = dir_year.joinpath(f"{game_id}.json")
    url_year = urljoin(config.URL_GAMES, str(year) + "/")
    url_game = urljoin(url_year, f"game/{game_id}/")
    parameters = {"include": "boxscore,play_by_play,rosters,penalties,play_reviews"}
    downloaded = set()
    if download_helper.downloader(url_game, filename_game, parameters=parameters, reset=reset):
        downloaded.add((year, game_id))
    return downloaded


def reset_advanced_all() -> None:
    """
    Re-download all advanced
    :return: State of local machine should now have copy of files (most recent)
    """
    download_advanced_year_range(config.YEAR_START_GAMES, config.YEAR_END_GAMES, reset=True)


def reset_advanced_year(year: int) -> None:
    """
    Re-download all advanced for indicated year
    :param year: The year
    :return: State of local machine should now have copy of file (most recent)
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    download_advanced_year(year, reset=True)


def reset_advanced_year_current() -> None:
    """
    Re-download all advanced for current year
    :return: State of local machine should now have copy of files (most recent)
    """
    reset_advanced_year(config.YEAR_CURRENT)


def reset_advanced_games(games: set[tuple[int, int]]) -> None:
    """
    Re-download all advanced indicated
    :param games: The set of (year, game_id) to download
    :return: State of local machine should now have copy of files (most recent)
    """
    if type(games) != set:
        raise ValueError(f"Type of year <{type(games)}> should be set!")
    download_advanced_games(games, reset=True)


def reset_advanced_games_active() -> None:
    """
    Re-download all advanced indicated as active
    :return: State of local machine should now have copy of file (most recent)
    """
    games = store_games.extract_games_active()
    reset_advanced_games(games)


def reset_advanced_game(game: tuple[int, int]) -> None:
    """
    Since there is only one file, this will re-download the one file to make sure it is recent
    :param game: The (year, game_id) to download
    :return: State of local machine should now have copy of file (most recent)
    """
    if type(game) != tuple:
        raise ValueError(f"Type of game <{type(game)}> should be tuple!")
    download_advanced_game(game, reset=True)


def main() -> None:
    """
    Used to test this file
    :return: None
    """
    # Reset advanced for all years
    if False:
        reset_advanced_all()
    # Reset advanced for a specific year
    if False:
        year = 2022
        reset_advanced_year(year)
    # Reset advanced for current year
    if False:
        reset_advanced_year_current()
    # Reset advanced for games
    if False:
        games = {(2022, 6227), (2022, 6228), (2022, 6229)}
        reset_advanced_games(games)
    # Reset advanced for active games
    if True:
        reset_advanced_games_active()
    # Reset advanced for game
    if False:
        game = (2022, 6229)
        reset_advanced_game(game)


if __name__ == '__main__':
    main()
    print("Done")
