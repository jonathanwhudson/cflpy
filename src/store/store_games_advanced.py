import json
from typing import Union

import pandas as pd

import config
import store_games
import store_helper
from store import store_columns, store_players
from store.parse import parse_advanced


def load_advanced_year_range(start: int, end: int) -> dict:
    """
    Load the advanced games json
    :param start: The first year
    :param end: The last year
    :return: Dictionary of dataframes
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not loading any advanced games as nothing in years=[{start},{end}]!")
    games = []
    for year in sorted(years):
        games.append(load_advanced_year(year, parse=False))
    games_df = pd.concat(games, ignore_index=True)
    return parse_advanced.parse_advanced_games(games_df)


def load_advanced_year(year: int, parse: bool = True) -> Union[dict, pd.DataFrame]:
    """
    Load the advanced games json
    :param year: The year
    :param parse: Whether to parse the files or return raw
    :return: Dictionary of dataframes
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    if type(parse) != bool:
        raise ValueError(f"Type of parse <{type(parse)}> should be bool!")
    games = store_games.extract_games_year(year)
    games_df = load_advanced_games(games, parse=False)
    if parse:
        return parse_advanced.parse_advanced_games(games_df)
    return games_df


def load_advanced_games(games: set[tuple[int, int]], parse: bool = True) -> Union[dict, pd.DataFrame]:
    """
    Load the advanced games json
    :param games: Which (year, game_id) to load (instead of them all)
    :param parse: Whether to parse the files or return raw
    :return: Dictionary of dataframes
    """
    if type(games) != set:
        raise ValueError(f"Type of limit <{type(games)}> should be set!")
    if type(parse) != bool:
        raise ValueError(f"Type of reset <{type(parse)}> should be bool!")
    games_temp = []
    for game in games:
        games_temp.append(load_advanced_game(game, parse=False))
    games_df = pd.concat(games_temp, ignore_index=True)
    if parse:
        return parse_advanced.parse_advanced_games(games_df)
    return games_df


def load_advanced_game(game: tuple[int, int], parse: bool = True) -> Union[dict, pd.DataFrame]:
    """
    Load the advanced game json
    :param game: (year, game_id) of game
    :param parse: Whether to parse the files or return raw
    :return: Dictionary of dataframes
    """
    if type(game) != tuple:
        raise ValueError(f"Type of game <{type(game)}> should be tuple!")
    if type(parse) != bool:
        raise ValueError(f"Type of reset <{type(parse)}> should be bool!")
    year, game_id = game
    dir_advanced = config.DIR_GAMES.joinpath("advanced")
    dir_year = dir_advanced.joinpath(str(year))
    filename_game = dir_year.joinpath(f"{game_id}.json")
    with open(filename_game) as file:
        game_df = pd.DataFrame(json.loads(file.read())['data'])
        if parse:
            return parse_advanced.parse_advanced_games(game_df)
        return game_df


def remove_advanced_year_range(start: int, end: int) -> None:
    """
    Remove the advanced games
    :param start: The first year
    :param end: The last year
    :return: None
    """
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not removing any advanced games as nothing in years=[{start},{end}]!")
    for year in years:
        games = store_games.extract_games_year(year)
        remove_advanced_games(games)


def remove_advanced_year(year: int) -> None:
    """
    Remove the advanced games
    :param year: The year
    :return: None
    """
    remove_advanced_year_range(year, year)


def remove_advanced_games(games: set[tuple[int, int]]):
    """
    Remove the advanced games
    :param games: Which (year, game_id) to Remove (instead of them all)
    :return: None
    """
    game_ids = {game_id for (year, game_id) in games}
    for table_name in store_columns.ADV_TABLES:
        store_helper.execute(f'''DELETE FROM {table_name} WHERE game_id in {str(tuple(game_ids)).replace(",)", ")")}''')


def remove_advanced_game(game: tuple[int, int]):
    """
    Remove the advanced game
    :param game: Which (year, game_id) to remove
    :return: None
    """
    remove_advanced_games({game})


def store_games_advanced(results, if_exists: str) -> None:
    """
    Since we get a dictionary back from parsing, we use this to do multiple storage of each dataframe in dictionary
    :param results: The dictionary of the tables to store
    :param if_exists: How to deal with table existing already
    :return: None
    """
    store_helper.store_dataframe(results['pbp'], "pbp",
                                 datatype=store_columns.convert_to_sql_types(store_columns.ADV_PBP),
                                 if_exists=if_exists)
    store_helper.store_dataframe(results['penalties'], 'penalties',
                                 datatype=store_columns.convert_to_sql_types(store_columns.ADV_PEN),
                                 if_exists=if_exists)
    store_helper.store_dataframe(results['reviews'], "reviews",
                                 datatype=store_columns.convert_to_sql_types(store_columns.ADV_REV),
                                 if_exists=if_exists)
    store_helper.store_dataframe(results['roster'], "roster",
                                 datatype=store_columns.convert_to_sql_types(store_columns.ADV_ROS),
                                 if_exists=if_exists)
    players_df = results['roster'][
        ['cfl_central_id', 'first_name', 'middle_name', 'last_name', 'birth_date']].drop_duplicates(keep="first").copy()
    store_players.update_with(players_df)
    for column_type, value in results['boxscore'].items():
        for column_name, df in value.items():
            store_helper.store_dataframe(df, f"{column_type}_{column_name}",
                                         datatype=store_columns.convert_to_sql_types(
                                             store_columns.ABV_BOX[column_type][column_name]), if_exists=if_exists)


def reset_advanced_all() -> None:
    """
    Reset all advanced
    :return: State of local machine should now have copy of files (most recent)
    """
    result = load_advanced_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    remove_advanced_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    store_games_advanced(result, store_helper.IF_EXISTS_REPLACE)


def reset_advanced_year(year: int) -> None:
    """
    Reset all advanced in given year
    :return: State of local machine should now have copy of files (most recent)
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    result = load_advanced_year(year, parse=True)
    remove_advanced_year(year)
    store_games_advanced(result, store_helper.IF_EXISTS_APPEND)


def reset_advanced_year_current() -> None:
    """
    Reset all advanced for current year
    :return: State of local machine should now have copy of files (most recent)
    """
    reset_advanced_year(config.YEAR_CURRENT)


def reset_advanced_games(games: set[tuple[int, int]]) -> None:
    """
    Reset all advanced indicated
    :param games: The set of (year, game_id) to reset
    :return: State of local machine should now have copy of files (most recent)
    """
    if type(games) != set:
        raise ValueError(f"Type of games <{type(games)}> should be set!")
    result = load_advanced_games(games, parse=True)
    remove_advanced_games(games)
    store_games_advanced(result, store_helper.IF_EXISTS_APPEND)


def reset_advanced_games_active() -> None:
    """
    Reset all advanced indicated as active
    :return: State of local machine should now have copy of file (most recent)
    """
    games = store_games.extract_games_active()
    if games:
        reset_advanced_games(games)


def reset_advanced_game(game: tuple[int, int]) -> None:
    """
    Since there is only one file, this will reset the one file to make sure it is recent
    :param game: The (year, game_id) to reset
    :return: State of local machine should now have copy of file (most recent)
    """
    if type(game) != tuple:
        raise ValueError(f"Type of game <{type(game)}> should be tuple!")
    result = load_advanced_game(game, parse=True)
    remove_advanced_game(game)
    store_games_advanced(result, store_helper.IF_EXISTS_APPEND)


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
