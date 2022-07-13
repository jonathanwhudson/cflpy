import pandas as pd

import config
from store import store_games, store_helper, store_columns
from store.parse import parse_gei


def load_gei_year_range(start: int, end: int) -> pd.DataFrame:
    """
    Load the GEI by processing EPA
    :param start: The first year
    :param end: The last year
    :return: GEI dataframe
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not loading any GEI as nothing in years=[{start},{end}]!")
    gei_df = load_gei(list(years), games=None)
    return gei_df


def load_gei_year(year: int) -> pd.DataFrame:
    """
    Load the GEI by processing EPA
    :param year: The year
    :return: GEI dataframe
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    if year == 2020:
        raise ValueError(f"Not loading GEI as year={year} did not get played!")
    gei_df = load_gei([year], games=None)
    return gei_df


def load_gei_games(games: set[int]) -> pd.DataFrame:
    """
    Load the GEI by processing EPA
    :param games: The games_ids
    :return: GEI dataframe
    """
    if type(games) != set:
        raise ValueError(f"Type of games <{type(games)}> should be set!")
    gei_df = load_gei(years=None, games=games)
    return gei_df


def load_gei_game(game_id: int) -> pd.DataFrame:
    """
    Load the GEI by processing EPA
    :param game_id: The game_id
    :return: GEI dataframe
    """
    if type(game_id) != int:
        raise ValueError(f"Type of game_id <{type(game_id)}> should be int!")
    gei_df = load_gei(years=None, games={game_id})
    return gei_df


def remove_gei_year_range(start: int, end: int) -> None:
    """
    Remove the GEI
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
        raise ValueError(f"Not removing any GEI as nothing in years=[{start},{end}]!")
    games = store_games.extract_games_year_range(start, end)
    games = {game_id for (year, game_id) in games}
    remove_gei_games(games)


def remove_gei_year(year: int) -> None:
    """
    Remove the GEI
    :param year: The year
    :return: None
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    games = store_games.extract_games_year(year)
    games = {game_id for (year, game_id) in games}
    remove_gei_games(games)


def remove_gei_games(games: set[int]):
    """
    Remove the GEI
    :param games: The game_ids to remove
    :return: None
    """
    if type(games) != set:
        raise ValueError(f"Type of games <{type(games)}> should be set!")
    store_helper.execute(f'''DELETE FROM gei WHERE game_id in {str(tuple(games)).replace(",)", ")")}''')


def remove_gei_game(game_id: int):
    """
    Remove the GEI
    :param game_id: The game_id to remove
    :return: None
    """
    if type(game_id) != int:
        raise ValueError(f"Type of game_id <{type(game_id)}> should be int!")
    remove_gei_games({game_id})


def load_gei(years: list[int] = None, games: set[int] = None) -> pd.DataFrame:
    """
    Load GEI from database
    :param years: If years use list of years as basis
    :param games: If game_ids use list of game_ids as basis
    :return: A loaded dataframe
    """
    if not years and not games:
        raise ValueError(f"Not loading any GEI as no year or game_ids were given!")
    add = ""
    if years:
        years = set(years)
        add += f"games.year in {str(tuple(years)).replace(',)', ')')}"
    if years and games:
        add += " AND "
    if games:
        games = set(games)
        add += f"""games.game_id in {str(tuple(games)).replace(",)", ")")}"""

    query = f"""SELECT epa.game_id,epa.year,epa.play_sequence,epa.team_2_wpa,epa.wp,games.team_1_score,games.team_2_score,drives.won FROM epa LEFT JOIN drives ON drives.play_id=epa.play_id and drives.entry=epa.entry LEFT JOIN games ON games.game_id=epa.game_id WHERE {add}"""

    pbp_df = store_helper.load_dataframe_from_query(query)
    gei_df = parse_gei.parse_gei(pbp_df)
    store_helper.add_missing_columns(gei_df, store_columns.GEI)
    store_helper.ensure_type_columns(gei_df, store_columns.GEI)
    store_helper.reorder_columns(gei_df, store_columns.GEI)
    return gei_df


def update_ranks() -> pd.DataFrame:
    """
    After calculating GEI, run this to get ranks for the GEI/GSI/CBF
    :return: The dataframe now has ranks
    """
    query = f"""SELECT * FROM gei"""
    gei_df = store_helper.load_dataframe_from_query(query)
    gei_df['gei_pct'] = gei_df['gei'].rank(pct=True)
    gei_df['gei_rank'] = gei_df['gei'].rank(ascending=False).astype(int)
    gei_df['gsi_pct'] = gei_df['gsi'].rank(pct=True)
    gei_df['gsi_rank'] = gei_df['gsi'].rank(ascending=False).astype(int)
    gei_df['cbf_pct'] = gei_df['cbf'].rank(pct=True)
    gei_df['cbf_rank'] = gei_df['cbf'].rank(ascending=False).astype(int)
    return gei_df


def reset_gei_all() -> None:
    """
    Store all GEI
    :return: Database will update GEI
    """
    gei_df = load_gei_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    remove_gei_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    store_helper.replace_dataframe(gei_df, "gei", datatype=store_columns.convert_to_sql_types(store_columns.GEI))
    gei_df = update_ranks()
    store_helper.replace_dataframe(gei_df, "gei", datatype=store_columns.convert_to_sql_types(store_columns.GEI))


def reset_gei_year(year: int) -> None:
    """
    Store all GEI for year
    :param year: Year to update
    :return: Database will update GEI
    """
    gei_df = load_gei_year(year)
    remove_gei_year(year)
    store_helper.append_dataframe(gei_df, "gei", datatype=store_columns.convert_to_sql_types(store_columns.GEI))
    gei_df = update_ranks()
    store_helper.replace_dataframe(gei_df, "gei", datatype=store_columns.convert_to_sql_types(store_columns.GEI))


def reset_gei_year_current() -> None:
    """
    Store all GEI for current year
    :return: Database will update GEI
    """
    reset_gei_year(config.YEAR_CURRENT)


def reset_gei_games(games: set[int]) -> None:
    """
    Store all GEI for games_ids
    :param games: games_ids to update
    :return: Database will update GEI
    """
    gei_df = load_gei_games(games)
    remove_gei_games(games)
    store_helper.append_dataframe(gei_df, "gei", datatype=store_columns.convert_to_sql_types(store_columns.GEI))
    gei_df = update_ranks()
    store_helper.replace_dataframe(gei_df, "gei", datatype=store_columns.convert_to_sql_types(store_columns.GEI))


def reset_gei_active() -> None:
    """
    Store all GEI for active games
    :return: Database will update GEI
    """
    games = store_games.extract_games_active()
    games = {game_id for (year, game_id) in games}
    if games:
        reset_gei_games(games)


def reset_gei_game(game: int) -> None:
    """
    Store all GEI for game
    :param game: Game to update
    :return: Database will update GEI
    """
    gei_df = load_gei_game(game)
    remove_gei_game(game)
    store_helper.append_dataframe(gei_df, "gei", datatype=store_columns.convert_to_sql_types(store_columns.GEI))
    gei_df = update_ranks()
    store_helper.replace_dataframe(gei_df, "gei", datatype=store_columns.convert_to_sql_types(store_columns.GEI))


def main() -> None:
    # Reset all gei (note that this should be followed by others that require GEI info)
    if False:
        reset_gei_all()
    # Reset all gei for current year (note that this should be followed by others that require GEI info)
    if False:
        reset_gei_year_current()
    # Reset all gei for chosen year (note that this should be followed by others that require GEI info)
    if False:
        year = 2022
        reset_gei_year(year)
    # Reset all gei for certain game(note that this should be followed by others that require GEI info)
    if False:
        game_ids = {6227, 6228, 6229}
        reset_gei_games(game_ids)
    # Reset gei for active games
    if True:
        reset_gei_active()
    # Reset all gei for certain game(note that this should be followed by others that require GEI info)
    if False:
        game_id = 6229
        reset_gei_game(game_id)


if __name__ == '__main__':
    main()
    print("Done")
