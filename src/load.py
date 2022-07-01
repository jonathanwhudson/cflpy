import datetime
import json
import os

import pandas as pd

import config
import parse_advanced
import parse_basic


def load_games_basic(start: int, end: int, limit: set[int] = None) -> pd.DataFrame:
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not loading anything as nothing in years=[{start},{end}]!")
    if limit and not limit.intersection(years):
        raise ValueError(f"Not loading anything as nothing in years=[{start},{end}] due to limit={limit}!")
    games = []
    for year in sorted(years):
        if not limit or year in limit:
            games.append(load_games_basic_year(year, limit))
    if not games:
        raise ValueError(f"Didn't load anything!")
    return pd.concat(games, ignore_index=True)


def load_games_basic_year(year: int, limit: set[int] = None) -> pd.DataFrame:
    if limit and year not in limit:
        raise ValueError(f"Not loading anything as year={year} not in limit={limit}!")
    dir_basic: os.path = config.DIR_GAMES.joinpath("basic")
    filename_year: os.path = dir_basic.joinpath(f"{year}.json")
    with open(filename_year) as file:
        return pd.DataFrame(json.loads(file.read())['data'])


def load_games_basic_parsed(start: int, end: int, limit=None) -> pd.DataFrame:
    return parse_basic.parse_basic_games(load_games_basic(start, end, limit))


def load_games_basic_year_parsed(year: int, limit=None) -> pd.DataFrame:
    return parse_basic.parse_basic_games(load_games_basic_year(year, limit))


def load_games_advanced(start: int, end: int, limit: set[int, int]) -> pd.DataFrame:
    if type(limit) == pd.DataFrame:
        limit = extract_year_game_id_pairs(limit)
    years = set(range(start, end + 1, 1))
    limit_years = {year for (year, game_id) in limit}
    if not years:
        raise ValueError(f"Not loading anything as nothing in years=[{start},{end}]!")
    if not limit_years.intersection(years):
        raise ValueError(
            f"Not loading anything as nothing in years=[{start},{end}] due to limit_years={limit_years}!")
    games = []
    for year in years:
        if year in limit_years:
            games.append(load_games_advanced_year(year, limit))
    if not games:
        raise ValueError(f"Didn't load anything!")
    return pd.concat(games, ignore_index=True)


def load_games_advanced_year(year: int, limit: set[int, int]) -> pd.DataFrame:
    if type(limit) == pd.DataFrame:
        limit = extract_year_game_id_pairs(limit)
    limit_years = {year for (year, game_id) in limit}
    if year not in limit_years:
        raise ValueError(f"Not loading anything as year={year} not in limit_years={limit_years}!")
    games = []
    for (game_year, game_id) in sorted(sorted(limit, key=lambda x: x[1]), key=lambda x: x[0]):
        if game_year == year:
            games.append(load_game_advanced(year, game_id, limit))
    if not games:
        raise ValueError(f"Didn't load anything!")
    return pd.concat(games, ignore_index=True)


def load_game_advanced(year: int, game_id: int, limit: set[int, int]) -> pd.DataFrame:
    if type(limit) == pd.DataFrame:
        limit = extract_year_game_id_pairs(limit)
    if (year, game_id) not in limit:
        raise ValueError(f"Not loading anything as ({year},{game_id}) not in limit={limit}!")
    dir_advanced: os.path = config.DIR_GAMES.joinpath("advanced")
    dir_year: os.path = dir_advanced.joinpath(str(year))
    filename_game: os.path = dir_year.joinpath(f"{game_id}.json")
    with open(filename_game) as file:
        return pd.DataFrame(json.loads(file.read())['data'])


def load_games_advanced_parsed(start: int, end: int, limit) -> dict:
    return parse_advanced.parse_advanced_games(load_games_advanced(start, end, limit))


def load_games_advanced_year_parsed(year: int, limit) -> dict:
    return parse_advanced.parse_advanced_games(load_games_advanced_year(year, limit))


def load_game_advanced_parsed(year: int, game_id: int, limit) -> dict:
    return parse_advanced.parse_advanced_games(load_game_advanced(year, game_id, limit))


def extract_year_game_id_pairs(dataframe: pd.DataFrame):
    return {(year, game_id) for [year, game_id] in dataframe[['year', 'game_id']].values.tolist()}


def extract_year_game_id_pairs_active(dataframe: pd.DataFrame):
    temp = dataframe.loc[((dataframe['event_status_id'] != 4) & (dataframe['event_status_id'] != 9)) & (dataframe['date_start'] < pd.to_datetime(datetime.datetime.utcnow()).tz_localize("UTC").tz_convert("UTC")), ['year', 'game_id']]
    return {(year, game_id) for [year, game_id] in temp.values.tolist()}


if __name__ == '__main__':
    games_df = load_games_basic_parsed(config.YEAR_START_GAMES, config.YEAR_END_GAMES)
    games = extract_year_game_id_pairs(games_df)
    result = load_games_advanced_parsed(config.YEAR_START_ADV, config.YEAR_END_ADV, games)
    print("Done")
