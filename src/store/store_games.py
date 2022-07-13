import json

import pandas as pd

import config
import store_columns
import store_helper
from store import store_teams
from store.parse import parse_games


def load_games_year_range(start: int, end: int) -> pd.DataFrame:
    """
    Load the games for given year range
    :param start: The first year
    :param end: The last year
    :return: A dataframe of the games
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not loading any games as nothing in years=[{start},{end}]!")
    games = []
    for year in sorted(years):
        games.append(load_games_year(year))
    games_df = pd.concat(games, ignore_index=True)
    return games_df


def load_games_year(year: int) -> pd.DataFrame:
    """
    Load the games for the given year
    :param year: The year
    :return: DataFrame of the current games for given year
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    dir_basic = config.DIR_GAMES.joinpath("basic")
    filename_year = dir_basic.joinpath(f"{year}.json")
    with open(filename_year) as file:
        games_df = parse_games.parse_games(pd.DataFrame(json.loads(file.read())['data']))
        return games_df


def removes_games_year_range(start: int, end: int) -> None:
    """
    Remove games for a particular range
    :param start: The first year
    :param end: The last year
    :return: Nothing but database modified to remove games between those years inclusive
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not removing any games as nothing in years=[{start},{end}]!")
    store_helper.execute(f'''DELETE FROM games WHERE year in {str(tuple(years)).replace(",)", ")")}''')


def remove_games_year(year: int):
    """
    Remove games for a particular year
    :param year: The  year
    :return: Nothing but database modified to remove games for that year
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    removes_games_year_range(year, year)


def reset_games_all() -> None:
    """
    Reset all the games
    :return: None
    """
    games_df = load_games_year_range(config.YEAR_START_GAMES, config.YEAR_END_GAMES)
    store_helper.replace_dataframe(games_df, "games", datatype=store_columns.convert_to_sql_types(store_columns.GAMES))
    # update_teams(games_df)


def reset_games_year(year: int) -> None:
    """
    Reset games for one year
    :return: None
    """
    games_df = load_games_year(year)
    remove_games_year(year)
    store_helper.append_dataframe(games_df, "games",
                                  datatype=store_columns.convert_to_sql_types(store_columns.GAMES))
    # update_teams(games_df)


def reset_games_year_current() -> None:
    """
    Reset games for current year
    :return: None
    """
    reset_games_year(config.YEAR_CURRENT)


def update_teams(games_df: pd.DataFrame) -> None:
    """
    Extract the teams from the dataframe and see if they should be added to the team database
    Some of these teams will be secondary entries for the same team_id
    :param games_df: The games dataframe
    :return: None
    """
    teams_1_df = \
        games_df.drop_duplicates(
            subset=["team_1_team_id", "team_1_abbreviation", "team_1_location", "team_1_nickname", "team_1_venue_id"])[
            ["team_1_team_id", "team_1_abbreviation", "team_1_location", "team_1_nickname", "team_1_venue_id"]].copy()
    teams_1_df.rename(
        columns={"team_1_team_id": "team_id", "team_1_abbreviation": "abbreviation", "team_1_location": "location",
                 "team_1_nickname": "nickname", "team_1_venue_id": "venue_id"}, inplace=True)
    store_teams.update_with(teams_1_df)
    teams_2_df = \
        games_df.drop_duplicates(
            subset=["team_2_team_id", "team_2_abbreviation", "team_2_location", "team_2_nickname", "team_2_venue_id"])[
            ["team_2_team_id", "team_2_abbreviation", "team_2_location", "team_2_nickname", "team_2_venue_id"]].copy()
    teams_2_df.rename(
        columns={"team_2_team_id": "team_id", "team_2_abbreviation": "abbreviation", "team_2_location": "location",
                 "team_2_nickname": "nickname", "team_2_venue_id": "venue_id"}, inplace=True)
    store_teams.update_with(teams_2_df)


def extract_games_year_range(start: int, end: int) -> set[tuple[int, int]]:
    """
    Get all (year,game_id)
    :param start: First year
    :param end: Last year
    :return: Set of all (year, game_id) in games
    """
    query = f"SELECT DISTINCT(game_id), year FROM games WHERE year >= {start} AND year <= {end}"
    return extract_helper(store_helper.load_dataframe_from_query(query))


def extract_games_current_year() -> set[tuple[int, int]]:
    """
    Get all (year,game_id) for current year
    :return: Set of all (year, game_id) in games for current year
    """
    return extract_games_year(config.YEAR_CURRENT)


def extract_games_year(year: int) -> set[tuple[int, int]]:
    """
    Get all (year,game_id)  for year
    :param year: Year to extract for
    :return: Set of all (year, game_id) in games for year
    """
    query = f"SELECT DISTINCT(game_id), year FROM games WHERE year = {year}"
    x = store_helper.load_dataframe_from_query(query)
    return extract_helper(x)


def extract_games_year_final(year: int) -> set[tuple[int, int]]:
    """
    Get all (year,game_id)  for year that are finalized
    :param year: Year to extract for
    :return: Set of all (year, game_id) in games for year that are finalized
    """
    query = f"SELECT DISTINCT(game_id), year FROM games WHERE year = {year} AND games.event_status_id=4"
    return extract_helper(store_helper.load_dataframe_from_query(query))


def extract_games_active() -> set[tuple[int, int]]:
    """
    Get all (year,game_id)  for year that are active (not final and not cancelled) and before now
    :return: Set of all (year, game_id) in games for year that are active (not final and not cancelled) and before now
    """
    query = f"SELECT DISTINCT(game_id), year FROM games WHERE (games.event_status_id != 4 AND games.event_status_id != 9) and games.date_start < strftime('%Y-%m-%dT%H:%M:%S-%f','now')"
    return extract_helper(store_helper.load_dataframe_from_query(query))


def extract_helper(dataframe: pd.DataFrame) -> set[tuple[int, int]]:
    """
    Converts a dataframe with year,game_ids into a set of tuples
    :param dataframe: The dataframe to convert
    :return: A set of tuples of (year, game_id) from dataframe
    """
    game_ids = dataframe[['year', 'game_id']].values.tolist()
    game_ids = {(year, game_id) for [year, game_id] in game_ids}
    return game_ids


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
