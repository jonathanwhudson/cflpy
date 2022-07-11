import json
import os
import sqlite3
from typing import Union

import pandas as pd

import column_dtypes
import config
import parse_advanced
import store

TABLES_ADVANCED = []
for name in column_dtypes.TEAM_COLUMNS:
    TABLES_ADVANCED.append("team_" + name)
for name in column_dtypes.PLAYER_COLUMNS:
    TABLES_ADVANCED.append("players_" + name)
TABLES_ADVANCED += ['pbp', 'penalties', 'reviews', 'roster']


def load_games_advanced_year_range(start: int, end: int, limit_years: set[int] = None) -> dict:
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not loading any advanced games as nothing in years=[{start},{end}]!")
    if limit_years:
        limit_years = set(limit_years)
        years = limit_years.intersection(years)
        if years:
            raise ValueError(
                f"Not loading any advanced games as nothing in years=[{start},{end}] due to limit={limit_years}!")
    games = []
    for year in sorted(years):
        games.append(load_games_advanced_year(year, parse=False))
    games_df = pd.concat(games, ignore_index=True)
    return parse_advanced.parse_advanced_games(games_df)


def load_games_advanced_year(year: int, parse: bool = True) -> Union[dict, pd.DataFrame]:
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(f"SELECT DISTINCT(game_id) FROM games WHERE year={year}", connection)
    game_ids = dataframe['game_id'].values.tolist()
    game_ids = [(year, game_id) for game_id in game_ids]
    games_df = load_games_advanced_games(game_ids, parse=False)
    if parse:
        return parse_advanced.parse_advanced_games(games_df)
    return games_df


def load_games_advanced_games(game_ids: list[tuple[int, int]], parse: bool = True) -> Union[dict, pd.DataFrame]:
    games = []
    for game_id in game_ids:
        games.append(load_games_advanced_game(game_id, parse=False))
    games_df = pd.concat(games, ignore_index=True)
    if parse:
        return parse_advanced.parse_advanced_games(games_df)
    return games_df


def load_games_advanced_game(game_id: tuple[int, int], parse: bool = True) -> Union[dict, pd.DataFrame]:
    year, game_id = game_id
    dir_advanced: os.path = config.DIR_GAMES.joinpath("advanced")
    dir_year: os.path = dir_advanced.joinpath(str(year))
    filename_game: os.path = dir_year.joinpath(f"{game_id}.json")
    with open(filename_game) as file:
        game_df = pd.DataFrame(json.loads(file.read())['data'])
        if parse:
            return parse_advanced.parse_advanced_games(game_df)
        return game_df


def remove_games_advanced_year_range(start: int, end: int, limit_years: list[int] = None) -> None:
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not removing any advanced games as nothing in years=[{start},{end}]!")
    if limit_years:
        limit_years = set(limit_years)
        years = limit_years.intersection(years)
        if years:
            raise ValueError(
                f"Not removing any advanced games as nothing in years=[{start},{end}] due to limit_years={limit_years}!")
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(
        f"""SELECT DISTINCT(game_id),year FROM games WHERE year in {str(tuple(years)).replace(",)", ")")}""",
        connection)
    game_ids = dataframe[['year', 'game_id']].values.tolist()
    game_ids = [(year, game_id) for [year, game_id] in game_ids]
    remove_game_advanced_games(game_ids)
    connection.close()


def remove_games_advanced_year(year: int) -> None:
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(f"SELECT DISTINCT(game_id), year FROM games WHERE year={year}", connection)
    game_ids = dataframe[['year', 'game_id']].values.tolist()
    game_ids = [(year, game_id) for [year, game_id] in game_ids]
    remove_game_advanced_games(game_ids)
    connection.close()


def remove_game_advanced_games(game_ids: list[tuple[int, int]]):
    game_ids = [game_id for (year, game_id) in game_ids]
    connection = sqlite3.connect(config.DB_FILE)
    for table_name in TABLES_ADVANCED:
        connection.execute(f'''DELETE FROM {table_name} WHERE game_id in {str(tuple(game_ids)).replace(",)", ")")}''')
        connection.commit()
    connection.close()


def remove_games_advanced_game(game_id: tuple[int, int]):
    remove_game_advanced_games([game_id])


def store_games_advanced(results, if_exists: str) -> None:
    store.store_dataframe(results['pbp'], "pbp", if_exists)
    store.store_dataframe(results['penalties'], 'penalties', if_exists)
    store.store_dataframe(results['reviews'], "reviews", if_exists)
    store.store_dataframe(results['roster'], "roster", if_exists)
    for column_type, value in results['boxscore'].items():
        for column_name, df in value.items():
            store.store_dataframe(df, f"{column_type}_{column_name}", if_exists)


def reset_advanced_games_all() -> None:
    result = load_games_advanced_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    remove_games_advanced_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    store_games_advanced(result, store.IF_EXISTS_REPLACE)


def reset_advanced_games_year(year: int) -> None:
    result = load_games_advanced_year(year, parse=True)
    remove_games_advanced_year(year)
    store_games_advanced(result, store.IF_EXISTS_APPEND)


def reset_advanced_games_current_year() -> None:
    reset_advanced_games_year(config.YEAR_CURRENT)


def reset_advanced_games_games(game_ids: list[tuple[int, int]]) -> None:
    result = load_games_advanced_games(game_ids, parse=True)
    remove_game_advanced_games(game_ids)
    store_games_advanced(result, store.IF_EXISTS_APPEND)


def reset_advanced_games_game(game_id: tuple[int, int]) -> None:
    result = load_games_advanced_game(game_id, parse=True)
    remove_games_advanced_game(game_id)
    store_games_advanced(result, store.IF_EXISTS_APPEND)

def extract_games():
    connection = sqlite3.connect(config.DB_FILE)
    query = f"SELECT DISTINCT(game_id), year FROM games"
    dataframe = pd.read_sql(query, connection)
    game_ids = dataframe[['year', 'game_id']].values.tolist()
    game_ids = [(year, game_id) for [year, game_id] in game_ids]
    return game_ids

def extract_games_current_year():
    return extract_games_year(config.YEAR_CURRENT)

def extract_games_year(year):
    connection = sqlite3.connect(config.DB_FILE)
    query = f"SELECT DISTINCT(game_id), year FROM games WHERE year = {year}"
    dataframe = pd.read_sql(query, connection)
    game_ids = dataframe[['year', 'game_id']].values.tolist()
    game_ids = [(year, game_id) for [year, game_id] in game_ids]
    return game_ids

def extract_games_active():
    connection = sqlite3.connect(config.DB_FILE)
    query = f"SELECT DISTINCT(game_id), year FROM games WHERE (games.event_status_id != 4 AND games.event_status_id != 9) and games.date_start < strftime('%Y-%m-%dT%H:%M:%S-%f','now')"
    dataframe = pd.read_sql(query, connection)
    game_ids = dataframe[['year', 'game_id']].values.tolist()
    game_ids = [(year, game_id) for [year, game_id] in game_ids]
    return game_ids

def reset_advanced_games_active() -> None:
    reset_advanced_games_games(extract_games_active())

def main() -> None:
    # Reset all advanced games (note that this should be followed by drives/EPA/GEI and others that require drive info)
    if False:
        print("Resetting all drives")
        reset_advanced_games_all()
    # Reset all advanced games for current year (note that this should be followed by drives/EPA/GEI and others that require drive info)
    if True:
        print("Resetting drives for current year")
        reset_advanced_games_current_year()
    # Reset all advanced games for chosen year (note that this should be followed by drives/EPA/GEI and others that require drive info)
    if False:
        year = 2022
        print(f"Resetting drives for year=<{year}>")
        reset_advanced_games_year(year)
    # Reset all active advanced games for certain game(note that this should be followed by drives/EPA/GEI and others that require drive info)
    if False:
        print(f"Resetting drives for active games")
        reset_advanced_games_active()
    # Reset all advanced games for certain game(note that this should be followed by drives/EPA/GEI and others that require drive info)
    if False:
        game_ids = [(2022, 6227), (2022, 6228), (2022, 6229)]
        print(f"Resetting drives for games=<{game_ids}>")
        reset_advanced_games_games(game_ids)
    # Reset all advanced games for certain game(note that this should be followed by drives/EPA/GEI and others that require drive info)
    if False:
        game_id = (2022, 6229)
        print(f"Resetting drives for game=<{game_id}>")
        reset_advanced_games_game(game_id)


if __name__ == '__main__':
    main()
    print("Done")
