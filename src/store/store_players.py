import json
import os
from typing import Union

import pandas as pd

import config
import store_columns
import store_helper


def load_players_year_range(start: int, end: int) -> pd.DataFrame:
    """
    Load the players for given year range
    :param start: The first year
    :param end: The last year
    :return: A dataframe of the players
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not storing any players as nothing in years=[{start},{end}]!")
    players = []
    for year in sorted(years):
        players.append(load_players_year(year))
    players_df = pd.concat(players, ignore_index=True)
    store_helper.add_missing_columns(players_df, store_columns.PLAYERS)
    store_helper.ensure_type_columns(players_df, store_columns.PLAYERS)
    store_helper.reorder_columns(players_df, store_columns.PLAYERS)
    return players_df


def load_players_year(year: Union[int, None]) -> pd.DataFrame:
    """
    Load the players for the given year
    :param year: The year
    :return: DataFrame of the current players for given year
    """
    if year and type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    if year:
        dir_year = config.DIR_PLAYERS.joinpath(str(year))
    else:
        dir_year = config.DIR_PLAYERS.joinpath("null")
    page = 1
    players = []
    while True:
        file_players = dir_year.joinpath(str(page) + ".json")
        if os.path.exists(file_players):
            with open(file_players) as file:
                players.append(pd.DataFrame(json.loads(file.read())['data']))
        else:
            break
        page += 1
    players_df = pd.concat(players, ignore_index=True)
    if not players_df.empty:
        players_df = store_helper.flatten(players_df, "school")
        players_df = store_helper.flatten(players_df, "position")
    if not players_df.empty:
        players_df.loc[players_df['cfl_central_id'] == 165959, 'height'] = 5.11
        players_df.loc[players_df['cfl_central_id'] == 164617, 'height'] = 6.03
        players_df.loc[players_df['cfl_central_id'] == 165832, 'birth_date'] = "1996-03-29 00:00:00"
    store_helper.add_missing_columns(players_df, store_columns.PLAYERS)
    store_helper.ensure_type_columns(players_df, store_columns.PLAYERS)
    store_helper.reorder_columns(players_df, store_columns.PLAYERS)
    return players_df


def remove_players_year_range(start: int, end: int) -> None:
    """
    Remove players for a particular range
    :param start: The first year
    :param end: The last year
    :return: Nothing but database modified to remove players between those years inclusive
    """
    if start and type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if end and type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    if not start or not end:
        store_helper.execute(f'''DELETE FROM players WHERE rookie_year IS NULL''')
    else:
        years = set(range(start, end + 1, 1))
        if not years:
            raise ValueError(f"Not removing any players games as nothing in years=[{start},{end}]!")
        store_helper.execute(f'''DELETE FROM players WHERE rookie_year in {str(tuple(years)).replace(",)", ")")}''')


def remove_players_year(year: Union[int, None]) -> None:
    """
    Remove players for a particular year
    :param year: The  year
    :return: Nothing but database modified to remove players for that year
    """
    remove_players_year_range(year, year)


def reset_players_all():
    """
    Reset all the players
    :return: None
    """
    players_df = load_players_year_range(config.YEAR_PLAYERS_MIN_ROOKIE_YEAR, config.YEAR_END_GAMES)
    store_helper.replace_dataframe(players_df, "players",
                                   datatype=store_columns.convert_to_sql_types(store_columns.PLAYERS))


def reset_players_year(year: int):
    """
    Reset players for one year
    :return: None
    """
    players_df = load_players_year(year)
    players_null_df = load_players_year(year=None)
    remove_players_year(year)
    remove_players_year(year=None)
    store_helper.append_dataframe(players_df, "players",
                                  datatype=store_columns.convert_to_sql_types(store_columns.PLAYERS))
    store_helper.append_dataframe(players_null_df, "players",
                                  datatype=store_columns.convert_to_sql_types(store_columns.PLAYERS))


def reset_players_year_current():
    """
    Reset players for current year
    :return: None
    """
    reset_players_year(config.YEAR_CURRENT)


def update_with(additional_players_df: pd.DataFrame) -> None:
    """
    Take a new setup of players and if any are new to database, then update it
    :param additional_players_df: A dataframe of correct format to update players table with
    :return: None
    """
    store_helper.ensure_type_columns(additional_players_df, store_columns.PLAYERS)
    store_helper.reorder_columns(additional_players_df, store_columns.PLAYERS)
    existing_players_df = store_helper.load_dataframe("players")[["cfl_central_id"]]
    store_helper.ensure_type_columns(existing_players_df, store_columns.PLAYERS)
    store_helper.reorder_columns(existing_players_df, store_columns.PLAYERS)
    difference_df = pd.concat([existing_players_df, existing_players_df, additional_players_df]).drop_duplicates(
        keep=False, subset=["cfl_central_id"])
    if not difference_df.empty:
        store_helper.append_dataframe(difference_df, "players",
                                      store_columns.convert_to_sql_types(store_columns.PLAYERS))


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
