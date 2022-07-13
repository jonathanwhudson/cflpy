import pandas as pd

import config
from model import model_epa
from store import store_games, store_helper, store_columns
from store.parse import parse_epa


def load_epa_year_range(start: int, end: int) -> pd.DataFrame:
    """
    Load the EPA by processing drives
    :param start: The first year
    :param end: The last year
    :return: EPA dataframe
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not loading any EPA as nothing in years=[{start},{end}]!")
    epa_df = load_epa(list(years), games=None)
    return epa_df


def load_epa_year(year: int) -> pd.DataFrame:
    """
    Load the EPA by processing play by play
    :param year: The year
    :return: EPA dataframe
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    if year == 2020:
        raise ValueError(f"Not loading EPA as year={year} did not get played!")
    epa_df = load_epa([year], games=None)
    return epa_df


def load_epa_games(games: set[int]) -> pd.DataFrame:
    """
    Load the EPA by processing play by play
    :param games: The games_ids
    :return: EPA dataframe
    """
    if type(games) != set:
        raise ValueError(f"Type of games <{type(games)}> should be set!")
    epa_df = load_epa(years=None, games=games)
    return epa_df


def load_epa_game(game_id: int) -> pd.DataFrame:
    """
    Load the EPA by processing play by play
    :param game_id: The game_id
    :return: EPA dataframe
    """
    if type(game_id) != int:
        raise ValueError(f"Type of game_id <{type(game_id)}> should be int!")
    epa_df = load_epa(years=None, games={game_id})
    return epa_df


def remove_epa_year_range(start: int, end: int) -> None:
    """
    Remove the EPA
    :param start: The first year
    :param end: The last year
    :return: None
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not removing any EPA as nothing in years=[{start},{end}]!")
    games = store_games.extract_games_year_range(start, end)
    games = {game_id for (year, game_id) in games}
    remove_epa_games(games)


def remove_epa_year(year: int) -> None:
    """
    Remove the EPA
    :param year: The year
    :return: None
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    games = store_games.extract_games_year(year)
    games = {game_id for (year, game_id) in games}
    remove_epa_games(games)


def remove_epa_games(games: set[int]):
    """
    Remove the EPA
    :param games: The game_ids to remove
    :return: None
    """
    if type(games) != set:
        raise ValueError(f"Type of games <{type(games)}> should be set!")
    store_helper.execute(f'''DELETE FROM epa WHERE game_id in {str(tuple(games)).replace(",)", ")")}''')


def remove_epa_game(game_id: int):
    """
    Remove the EPA
    :param game_id: The game_id to remove
    :return: None
    """
    if type(game_id) != int:
        raise ValueError(f"Type of game_id <{type(game_id)}> should be int!")
    remove_epa_games({game_id})


def load_epa(years: list[int] = None, games: set[int] = None) -> pd.DataFrame:
    """
    Load EPA from database
    :param years: If years use list of years as basis
    :param games: If game_ids use list of game_ids as basis
    :return: A loaded dataframe
    """
    if not years and not games:
        raise ValueError(f"Not loading any EPA as no year or game_ids were given!")
    add = ""
    if years:
        years = set(years)
        add += f"games.year in {str(tuple(years)).replace(',)', ')')}"
    if years and games:
        add += " AND "
    if games:
        game_ids = set(games)
        add += f"""games.game_id in {str(tuple(game_ids)).replace(",)", ")")}"""
    query = f"""SELECT drives.*, pbp.play_summary, pbp.play_result_type_id,games.team_1_is_winner,games.team_2_is_winner FROM drives LEFT JOIN pbp ON pbp.play_id=drives.play_id and drives.entry=pbp.entry LEFT JOIN games ON games.game_id=drives.game_id WHERE {add}"""
    epa_df = store_helper.load_dataframe_from_query(query)
    epa_df = parse_epa.parse_epa(epa_df)
    store_helper.add_missing_columns(epa_df, store_columns.EPA)
    store_helper.ensure_type_columns(epa_df, store_columns.EPA)
    store_helper.reorder_columns(epa_df, store_columns.EPA)
    return epa_df


def reset_epa_all() -> None:
    """
    Store all EPA
    :return: Database will update EPA
    """
    epa_df = load_epa_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    remove_epa_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    store_helper.replace_dataframe(epa_df, "epa", datatype=store_columns.convert_to_sql_types(store_columns.EPA))


def reset_epa_year(year: int) -> None:
    """
    Store all EPA for year
    :param year: Year to update
    :return: Database will update EPA
    """
    epa_df = load_epa_year(year)
    remove_epa_year(year)
    store_helper.append_dataframe(epa_df, "epa", datatype=store_columns.convert_to_sql_types(store_columns.EPA))


def reset_epa_year_current() -> None:
    """
    Store all EPA for current year
    :return: Database will update EPA
    """
    reset_epa_year(config.YEAR_CURRENT)


def reset_epa_games(games: set[int]) -> None:
    """
    Store all EPA for games_ids
    :param games: games_ids to update
    :return: Database will update EPA
    """
    epa_df = load_epa_games(games)
    remove_epa_games(games)
    store_helper.append_dataframe(epa_df, "epa", datatype=store_columns.convert_to_sql_types(store_columns.EPA))


def reset_epa_active() -> None:
    """
    Store all EPA for active games
    :return: Database will update EPA
    """
    games = store_games.extract_games_active()
    games = {game_id for (year, game_id) in games}
    if games:
        reset_epa_games(games)


def reset_epa_game(game: int) -> None:
    """
    Store all EPA for game
    :param game: Game to update
    :return: Database will update EPA
    """
    epa_df = load_epa_game(game)
    remove_epa_game(game)
    store_helper.append_dataframe(epa_df, "epa", datatype=store_columns.convert_to_sql_types(store_columns.EPA))


def main() -> None:
    # Reset all epa (note that this should be followed by GEI and others that require EPA info)
    if False:
        reset_epa_all()
    # Reset all epa for current year (note that this should be followed by GEI and others that require EPA info)
    if False:
        reset_epa_year_current()
    # Reset all epa for chosen year (note that this should be followed by GEI and others that require EPA info)
    if False:
        year = 2022
        reset_epa_year(year)
    # Reset all epa for certain game(note that this should be followed by GEI and others that require EPA info)
    if False:
        game_ids = {6227, 6228, 6229}
        reset_epa_games(game_ids)
    # Reset epa for active games
    if True:
        reset_epa_active()
    # Reset all epa for certain game(note that this should be followed by GEI and others that require EPA info)
    if False:
        game_id = 6229
        reset_epa_game(game_id)


if __name__ == '__main__':
    main()
    print("Done")
