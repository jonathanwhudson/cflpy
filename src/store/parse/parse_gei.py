import pandas as pd

from store import store_helper


def parse_gei(pbp_df: pd.DataFrame) -> pd.DataFrame:
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
        ['year', 'game_id', 'gei', 'gsi', 'cbf', 'gei_pct', 'gsi_pct', 'cbf_pct', 'gei_rank', 'gsi_rank',
         'cbf_rank']].copy()
    return gei_df


def calc_average_plays_game() -> float:
    """
    Calculate how many plays are in an average game
    :return: Average plays a game
    """
    query_all = f"""SELECT epa.year,epa.game_id,epa.play_sequence FROM epa LEFT JOIN games ON games.game_id=epa.game_id  WHERE games.event_type_id > 0 AND epa.year>={config.YEAR_START_ADV_USEFUL}"""
    pbp_all_df = store_helper.load_dataframe_from_query(query_all)
    average_plays_game = pbp_all_df.groupby(["year", "game_id"]).agg({'play_sequence': "count"}).mean().iat[0]
    return average_plays_game


def calc_average_total_score_game() -> float:
    """
    Calculate the average total score in a game
    :return: The average to total score in a game
    """
    query_all = f"""SELECT epa.year,epa.game_id,games.team_1_score,games.team_2_score FROM epa LEFT JOIN games ON games.game_id=epa.game_id WHERE games.event_type_id > 0 AND games.year>={config.YEAR_START_ADV_USEFUL}"""
    pbp_all_df = store_helper.load_dataframe_from_query(query_all)
    gsi_all_df = pbp_all_df.groupby(["year", "game_id"]).agg({"team_1_score": "max", "team_2_score": "max"})
    gsi_all_df['total_score'] = gsi_all_df['team_1_score'] + gsi_all_df['team_2_score']
    average_total_score_game = gsi_all_df['total_score'].mean()
    return average_total_score_game
