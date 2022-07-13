import pandas as pd

import config
from store import store_columns, store_helper, store_games
from store.parse import parse_drives


def load_drives_year_range(start: int, end: int) -> pd.DataFrame:
    """
    Load the drives by processing play by play
    :param start: The first year
    :param end: The last year
    :return: Drives dataframe
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not loading any drives as nothing in years=[{start},{end}]!")
    drives_df = load_drives(years, games=None)
    return drives_df


def load_drives_year(year: int) -> pd.DataFrame:
    """
    Load the drives by processing play by play
    :param year: The year
    :return: Drives dataframe
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    if year == 2020:
        raise ValueError(f"Not loading drives as year={year} did not get played!")
    drives_df = load_drives({year}, games=None)
    return drives_df


def load_drives_games(games: set[int]) -> pd.DataFrame:
    """
    Load the drives by processing play by play
    :param games: The games_ids
    :return: Drives dataframe
    """
    if type(games) != set:
        raise ValueError(f"Type of games <{type(games)}> should be set!")
    drives_df = load_drives(years=None, games=games)
    return drives_df


def load_drives_game(game_id: int) -> pd.DataFrame:
    """
    Load the drives by processing play by play
    :param game_id: The game_id
    :return: Drives dataframe
    """
    if type(game_id) != int:
        raise ValueError(f"Type of game_id <{type(game_id)}> should be int!")
    drives_df = load_drives(years=None, games={game_id})
    return drives_df


def remove_drives_year_range(start: int, end: int) -> None:
    """
    Remove the drives
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
        raise ValueError(f"Not removing any drives as nothing in years=[{start},{end}]!")
    games = store_games.extract_games_year_range(start, end)
    games = {game_id for (year, game_id) in games}
    remove_drives_games(games)


def remove_drives_year(year: int) -> None:
    """
    Remove the drives
    :param year: The year
    :return: None
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    games = store_games.extract_games_year(year)
    games = {game_id for (year, game_id) in games}
    remove_drives_games(games)


def remove_drives_games(games: set[int]) -> None:
    """
    Remove the drives
    :param games: The game_ids to remove
    :return: None
    """
    if type(games) != set:
        raise ValueError(f"Type of games <{type(games)}> should be set!")
    store_helper.execute(f'''DELETE FROM drives WHERE game_id in {str(tuple(games)).replace(",)", ")")}''')


def remove_drives_game(game_id: int):
    """
    Remove the drives
    :param game_id: The game_id to remove
    :return: None
    """
    if type(game_id) != int:
        raise ValueError(f"Type of game_id <{type(game_id)}> should be int!")
    remove_drives_games({game_id})


def load_drives(years: set[int] = None, games: set[int] = None) -> pd.DataFrame:
    """
    Load drives from database
    :param years: If years use list of years as basis
    :param games: If game_ids use list of game_ids as basis
    :return: A loaded dataframe
    """
    if not years and not games:
        raise ValueError(f"Not loading any drives as no year or game_ids were given!")
    add = ""
    if years:
        years = set(years)
        add += f"games.year in {str(tuple(years)).replace(',)', ')')}"
    if years and games:
        add += " AND "
    if games:
        game_ids = set(games)
        add += f"""games.game_id in {str(tuple(game_ids)).replace(",)", ")")}"""
    query = f"""SELECT pbp.*,games.year,games.team_1_team_id, games.team_1_score, games.team_1_is_at_home, games.team_1_is_winner,
         games.team_2_team_id, games.team_2_abbreviation, games.team_2_score, games.team_2_is_at_home, games.team_2_is_winner FROM pbp LEFT JOIN games ON pbp.game_id=games.game_id WHERE {add};"""
    pbp_df = store_helper.load_dataframe_from_query(query)
    drives_df = parse_drives.parse_drives(pbp_df)
    store_helper.add_missing_columns(drives_df, store_columns.DRIVES)
    store_helper.ensure_type_columns(drives_df, store_columns.DRIVES)
    store_helper.reorder_columns(drives_df, store_columns.DRIVES)
    return drives_df


def reset_drives_all() -> None:
    """
    Store all drives
    :return: Database will update drives
    """
    drives_df = load_drives_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    remove_drives_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    store_helper.replace_dataframe(drives_df, "drives",
                                   datatype=store_columns.convert_to_sql_types(store_columns.DRIVES))


def reset_drives_year(year: int) -> None:
    """
    Store all drives for year
    :param year:
    :return: Database will update drives
    """
    drives_df = load_drives_year(year)
    remove_drives_year(year)
    store_helper.append_dataframe(drives_df, "drives",
                                  datatype=store_columns.convert_to_sql_types(store_columns.DRIVES))


def reset_drives_year_current() -> None:
    """
    Store all drives for current year
    :return: Database will update drives
    """
    reset_drives_year(config.YEAR_CURRENT)


def reset_drives_games(games: set[int]) -> None:
    """
    Store all drives for games_ids
    :param games: 
    :return: Database will update drives
    """
    drives_df = load_drives_games(games)
    remove_drives_games(games)
    store_helper.append_dataframe(drives_df, "drives",
                                  datatype=store_columns.convert_to_sql_types(store_columns.DRIVES))


def reset_drives_active() -> None:
    """
    Store all drives for active games
    :return: Database will update drives
    """
    games = store_games.extract_games_active()
    games = {game_id for (year, game_id) in games}
    if games:
        reset_drives_games(games)


def reset_drives_game(game: int) -> None:
    """
    Store all drives for game
    :param game: 
    :return: Database will update drives
    """
    drives_df = load_drives_game(game)
    remove_drives_game(game)
    store_helper.append_dataframe(drives_df, "drives",
                                  datatype=store_columns.convert_to_sql_types(store_columns.DRIVES))


def main() -> None:
    # Reset all drives (note that this should be followed by EPA/GEI and others that require drive info)
    if False:
        reset_drives_all()
    # Reset all drives for current year (note that this should be followed by EPA/GEI and others that require drive info)
    if False:
        reset_drives_year_current()
    # Reset all drives for chosen year (note that this should be followed by EPA/GEI and others that require drive info)
    if False:
        year = 2022
        reset_drives_year(year)
    # Reset all drives for certain game(note that this should be followed by EPA/GEI and others that require drive info)
    if False:
        games = {6227, 6228, 6229}
        reset_drives_games(games)
    # Reset advanced for active games
    if True:
        reset_drives_active()
    # Reset all drives for certain game(note that this should be followed by EPA/GEI and others that require drive info)
    if False:
        game_id = 6229
        reset_drives_game(game_id)


if __name__ == '__main__':
    main()
    print("Done")
