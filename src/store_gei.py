import sqlite3

import pandas as pd

import column_dtypes
import config
import helper
import store


def load_gei_year_range(start: int, end: int, limit_years: list[int] = None) -> pd.DataFrame:
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not loading any GEI as nothing in years=[{start},{end}]!")
    if limit_years:
        limit_years = set(limit_years)
        years = years.intersection(limit_years)
        if not years:
            raise ValueError(
                f"Not loading any GEI as nothing in years=[{start},{end}] due to limit_years={limit_years}!")
    gei_df = load_gei(list(years), game_ids=None)
    return helper.clean_and_order_columns(gei_df, column_dtypes.GEI_COLUMNS)


def load_gei_year(year: int) -> pd.DataFrame:
    if year == 2020:
        raise ValueError(f"Not loading GEI as year={year} did not get played!")
    gei_df = load_gei([year], game_ids=None)
    return helper.clean_and_order_columns(gei_df, column_dtypes.GEI_COLUMNS)


def load_gei_games(game_ids: list[int]) -> pd.DataFrame:
    gei_df = load_gei(years=None, game_ids=game_ids)
    return helper.clean_and_order_columns(gei_df, column_dtypes.GEI_COLUMNS)


def load_gei_game(game_id: int) -> pd.DataFrame:
    gei_df = load_gei(years=None, game_ids=[game_id])
    return helper.clean_and_order_columns(gei_df, column_dtypes.GEI_COLUMNS)


def remove_gei_year_range(start: int, end: int, limit_years: list[int] = None) -> None:
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not removing any GEI as nothing in years=[{start},{end}]!")
    if limit_years:
        limit_years = set(limit_years)
        years = limit_years.intersection(years)
        if years:
            raise ValueError(
                f"Not removing any GEI as nothing in years=[{start},{end}] due to limit_years={limit_years}!")
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(
        f"""SELECT DISTINCT(game_id) FROM gei WHERE year in {str(tuple(years)).replace(",)", ")")}""",
        connection)
    connection.close()
    game_ids = dataframe['game_id'].values.tolist()
    remove_gei_games(game_ids)


def remove_gei_year(year: int) -> None:
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(f"SELECT DISTINCT(game_id) FROM gei WHERE year={year}", connection)
    game_ids = dataframe['game_id'].values.tolist()
    connection.close()
    remove_gei_games(game_ids)


def remove_gei_games(game_ids: list[int]):
    game_ids = set(game_ids)
    connection = sqlite3.connect(config.DB_FILE)
    connection.execute(f'''DELETE FROM gei WHERE game_id in {str(tuple(game_ids)).replace(",)", ")")}''')
    connection.commit()
    connection.close()


def remove_gei_game(game_id: int):
    remove_gei_games([game_id])


def calc_average_plays_game() -> float:
    query_all = f"""SELECT epa.year,epa.game_id,epa.play_sequence FROM epa LEFT JOIN games ON games.game_id=epa.game_id  WHERE games.event_type_id > 0 AND epa.year>={config.YEAR_START_ADV_USEFUL}"""
    pbp_all_df = store.query(query_all)
    average_plays_game = pbp_all_df.groupby(["year", "game_id"]).agg({'play_sequence': "count"}).mean().iat[0]
    return average_plays_game


def calc_average_total_score_game() -> float:
    query_all = f"""SELECT epa.year,epa.game_id,games.team_1_score,games.team_2_score FROM epa LEFT JOIN games ON games.game_id=epa.game_id WHERE games.event_type_id > 0 AND games.year>={config.YEAR_START_ADV_USEFUL}"""
    pbp_all_df = store.query(query_all)
    gsi_all_df = pbp_all_df.groupby(["year", "game_id"]).agg({"team_1_score": "max", "team_2_score": "max"})
    gsi_all_df['total_score'] = gsi_all_df['team_1_score'] + gsi_all_df['team_2_score']
    average_total_score_game = gsi_all_df['total_score'].mean()
    return average_total_score_game


def load_gei(years: list[int] = None, game_ids: list[int] = None) -> pd.DataFrame:
    if not years and not game_ids:
        raise ValueError(f"Not loading any GEI as no year or game_ids were given!")
    add = ""
    if years:
        years = set(years)
        add += f"games.year in {str(tuple(years)).replace(',)', ')')}"
    if years and game_ids:
        add += " AND "
    if game_ids:
        game_ids = set(game_ids)
        add += f"""games.game_id in {str(tuple(game_ids)).replace(",)", ")")}"""

    query = f"""SELECT epa.game_id,epa.year,epa.play_sequence,epa.team_2_wpa,epa.wp,games.team_1_score,games.team_2_score,drives.won FROM epa LEFT JOIN drives ON drives.play_id=epa.play_id and drives.entry=epa.entry LEFT JOIN games ON games.game_id=epa.game_id WHERE {add}"""

    pbp_df = store.query(query)
    pbp_df['won'] = pbp_df['won'].astype(bool)

    # average_plays_game = 159.23708
    average_plays_game = calc_average_plays_game()
    average_plays_game_factor = pbp_df.groupby(["year", "game_id"]).agg({'play_sequence': "count"}).apply(
        lambda x: average_plays_game / x)
    pbp_df['gei'] = pbp_df['team_2_wpa'].abs()
    gei_df = pbp_df.groupby(["year", "game_id"]).agg({'gei': "sum"})

    gei_df = pd.concat([average_plays_game_factor, gei_df], axis=1)
    gei_df['gei'] = gei_df['play_sequence'] * gei_df['gei']

    # average_total_score_game = 50.73657
    average_total_score_game = calc_average_total_score_game()

    gsi_df = pbp_df.groupby(["year", "game_id"]).agg({"team_1_score": "max", "team_2_score": "max"})
    gsi_df['total_score'] = gsi_df['team_1_score'] + gsi_df['team_2_score']
    gsi_df['gsi'] = gsi_df['total_score'] / average_total_score_game

    pbp_df.loc[pbp_df['won'], "cbf"] = pbp_df['wp']
    pbp_df.loc[~pbp_df['won'], "cbf"] = 1 - pbp_df['wp']

    cbf_df = pbp_df.groupby(["year", "game_id"]).agg({'cbf': "min"})
    cbf_df['cbf'] = 1 / cbf_df['cbf']

    games_df = pd.concat([gei_df, gsi_df, cbf_df], axis=1)
    del gei_df, gsi_df, cbf_df, average_total_score_game, average_plays_game, average_plays_game_factor
    games_df['gsi'] = games_df['gei'] * games_df['gsi']
    games_df['gei_pct'] = 0
    games_df['gei_rank'] = 0
    games_df['gsi_pct'] = 0
    games_df['gsi_rank'] = 0
    games_df['cbf_pct'] = 0
    games_df['cbf_rank'] = 0
    games_df.drop(['play_sequence', 'team_1_score', 'team_2_score', 'total_score'], axis=1, inplace=True)
    games_df.reset_index(inplace=True)
    gei_df = games_df[
        ['year', 'game_id', 'gei', 'gsi', 'cbf', 'gei_pct', 'gsi_pct', 'cbf_pct', 'gei_rank', 'gsi_rank', 'cbf_rank']]
    return gei_df


def update_ranks() -> pd.DataFrame:
    query = f"""SELECT * FROM gei"""
    gei_df = store.query(query)
    gei_df['gei_pct'] = gei_df['gei'].rank(pct=True)
    gei_df['gei_rank'] = gei_df['gei'].rank(ascending=False).astype(int)
    gei_df['gsi_pct'] = gei_df['gsi'].rank(pct=True)
    gei_df['gsi_rank'] = gei_df['gsi'].rank(ascending=False).astype(int)
    gei_df['cbf_pct'] = gei_df['cbf'].rank(pct=True)
    gei_df['cbf_rank'] = gei_df['cbf'].rank(ascending=False).astype(int)
    return gei_df


def store_gei(gei_df: pd.DataFrame, if_exists: str = store.IF_EXISTS_REPLACE):
    store.store_dataframe(gei_df, "gei", if_exists=if_exists)


def reset_gei_all() -> None:
    gei_df = load_gei_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    remove_gei_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    store_gei(gei_df, store.IF_EXISTS_REPLACE)
    gei_df = update_ranks()
    store_gei(gei_df, store.IF_EXISTS_REPLACE)


def reset_gei_year(year: int) -> None:
    gei_df = load_gei_year(year)
    remove_gei_year(year)
    store_gei(gei_df, store.IF_EXISTS_APPEND)
    gei_df = update_ranks()
    store_gei(gei_df, store.IF_EXISTS_REPLACE)


def reset_gei_current_year() -> None:
    reset_gei_year(config.YEAR_CURRENT)


def reset_gei_games(game_ids: list[int]) -> None:
    gei_df = load_gei_games(game_ids)
    remove_gei_games(game_ids)
    store_gei(gei_df, store.IF_EXISTS_APPEND)
    gei_df = update_ranks()
    store_gei(gei_df, store.IF_EXISTS_REPLACE)


def reset_gei_game(game_id: int) -> None:
    gei_df = load_gei_game(game_id)
    remove_gei_game(game_id)
    store_gei(gei_df, store.IF_EXISTS_APPEND)
    gei_df = update_ranks()
    store_gei(gei_df, store.IF_EXISTS_REPLACE)


def main() -> None:
    # Reset all gei (note that this should be followed by others that require GEI info)
    if False:
        print("Resetting all gei")
        reset_gei_all()
    # Reset all gei for current year (note that this should be followed by others that require GEI info)
    if True:
        print("Resetting gei for current year")
        reset_gei_current_year()
    # Reset all gei for chosen year (note that this should be followed by others that require GEI info)
    if False:
        year = 2022
        print(f"Resetting gei for year=<{year}>")
        reset_gei_year(year)
    # Reset all gei for certain game(note that this should be followed by others that require GEI info)
    if False:
        game_ids = [6227, 6228, 6229]
        print(f"Resetting gei for games=<{game_ids}>")
        reset_gei_games(game_ids)
    # Reset all gei for certain game(note that this should be followed by others that require GEI info)
    if False:
        game_id = 6229
        print(f"Resetting gei for game=<{game_id}>")
        reset_gei_game(game_id)


if __name__ == '__main__':
    main()
    print("Done")
