import json
import os
import sqlite3

import numpy as np
import pandas as pd

import config
import helper
import column_dtypes
import parse_basic

IF_EXISTS_FAIL = "fail"
IF_EXISTS_REPLACE = "replace"
IF_EXISTS_APPEND = "append"


def query(query_string: str):
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(query_string, connection)
    connection.close()
    return dataframe


def store_dataframe(df: pd.DataFrame, table_name: str, if_exists: str = "fail") -> None:
    helper.mkdir(config.DB_DIR)
    connection = sqlite3.connect(config.DB_FILE)
    df.to_sql(table_name, connection, if_exists=if_exists, index=False)
    connection.close()


def load_games_basic_years_range(start: int, end: int, limit_years: list[int] = None) -> pd.DataFrame:
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not loading any basic games as nothing in years=[{start},{end}]!")
    if limit_years:
        limit_years = set(limit_years)
        years = limit_years.intersection(years)
        if years:
            raise ValueError(
                f"Not loading any basic games as nothing in years=[{start},{end}] due to limit={limit_years}!")
    games = []
    for year in sorted(years):
        games.append(load_games_basic_year(year))
    games_df = pd.concat(games, ignore_index=True)
    return helper.clean_and_order_columns(games_df, column_dtypes.GAMES_BASIC)


def load_games_basic_year(year: int) -> pd.DataFrame:
    dir_basic: os.path = config.DIR_GAMES.joinpath("basic")
    filename_year: os.path = dir_basic.joinpath(f"{year}.json")
    with open(filename_year) as file:
        games_df = parse_basic.parse_basic_games(pd.DataFrame(json.loads(file.read())['data']))
        return helper.clean_and_order_columns(games_df, column_dtypes.GAMES_BASIC)



def removes_games_basic_years_range(start: int, end: int, limit_years: list[int] = None) -> None:
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not removing any basic games as nothing in years=[{start},{end}]!")
    if limit_years:
        limit_years = set(limit_years)
        years = limit_years.intersection(years)
        if years:
            raise ValueError(
                f"Not removing any basic games as nothing in years=[{start},{end}] due to limit_years={limit_years}!")
    connection = sqlite3.connect(config.DB_FILE)
    connection.execute(f'''DELETE FROM games WHERE year in {str(tuple(years)).replace(",)", ")")}''')
    connection.commit()
    connection.close()


def remove_games_basic_year(year: int):
    connection = sqlite3.connect(config.DB_FILE)
    connection.execute(f'''DELETE FROM games WHERE year in {str((year,)).replace(",)", ")")}''')
    connection.commit()
    connection.close()


def remove_games_basic_games(game_ids: list[int]):
    connection = sqlite3.connect(config.DB_FILE)
    connection.execute(f'''DELETE FROM games WHERE game_id in {str(tuple(game_ids)).replace(",)", ")")}''')
    connection.commit()
    connection.close()


def remove_games_basic_game(game_id: int):
    remove_games_basic_games([game_id])


def store_games_basic(dataframe: pd.DataFrame, if_exists: str) -> None:
    store_dataframe(dataframe, "games", if_exists)


def reset_all_games() -> None:
    games_df = load_games_basic_years_range(config.YEAR_START_GAMES, config.YEAR_END_GAMES)
    removes_games_basic_years_range(config.YEAR_START_GAMES, config.YEAR_END_GAMES)
    store_games_basic(games_df, IF_EXISTS_REPLACE)


def reset_games_year(year: int) -> None:
    games_df = load_games_basic_year(year)
    remove_games_basic_year(year)
    store_games_basic(games_df, IF_EXISTS_APPEND)


def reset_games_current_year() -> None:
    reset_games_year(config.YEAR_CURRENT)

def main() -> None:
    # Reset all games (note that this should be followed by advanced game info/drives/EPA/GEI and others that require drive info)
    if False:
        print("Resetting all games")
        reset_all_games()
    # Reset all games for current year (note that this should be followed by advanced game info/drives/EPA/GEI and others that require drive info)
    if True:
        print("Resetting games for current year")
        reset_games_current_year()
    # Reset all games for chosen year (note that this should be followed by advanced game info/drives/EPA/GEI and others that require drive info)
    if False:
        year = 2022
        print(f"Resetting games for year=<{year}>")
        reset_games_year(year)


if __name__ == '__main__':
    #main()
    temp = extract_year_game_id_pairs_active()
    print(temp)
    print("Done")
