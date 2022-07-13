import numpy as np
import pandas as pd

from store import store_helper, store_columns
from store.parse import parse_helper


def parse_games(games_df: pd.DataFrame) -> pd.DataFrame:
    """
    Take in a messy dataframe from json and clean it up
    :param games_df: The dataframe to clean
    :return: The cleaned dataframe
    """
    # Set as datetime ISO 8601 format with timezone in UTC
    games_df['date_start'] = pd.to_datetime(games_df['date_start'], format='%Y-%m-%dT%H:%M:%S%Z', utc=True)
    # Pull year as it is so common of filter
    games_df['year'] = games_df['date_start'].dt.year
    # Handle event type (also has possible title for game)
    games_df = parse_helper.flatten(games_df, "event_type")
    games_df.rename(columns={'name': 'event_type', "title": "event_title"}, inplace=True)
    # Handle event status
    games_df = parse_helper.flatten(games_df, "event_status")
    games_df.rename(columns={'name': 'event_status'}, inplace=True)
    # Handle venue
    games_df = parse_helper.flatten(games_df, "venue")
    games_df.rename(columns={'name': 'venue'}, inplace=True)
    # Handle weather
    games_df = parse_helper.flatten(games_df, "weather")
    # Handle coin toss
    games_df = parse_helper.flatten(games_df, "coin_toss")
    # Start with team information flattening
    games_df = pd.concat([games_df, pd.DataFrame(games_df['team_1'].values.tolist()).add_prefix("team_1_")], axis=1)
    games_df = pd.concat([games_df, pd.DataFrame(games_df['team_2'].values.tolist()).add_prefix("team_2_")], axis=1)
    games_df.drop(["team_1", "team_2"], axis=1, inplace=True)
    # Then linescore flattening
    games_df['team_1_q1'] = pd.DataFrame(
        [x[0][0]['score'] if x[0] else None for x in games_df[['team_1_linescores']].values])
    games_df['team_1_q2'] = pd.DataFrame(
        [x[0][1]['score'] if x[0] and len(x[0]) > 1 else None for x in games_df[['team_1_linescores']].values])
    games_df['team_1_q3'] = pd.DataFrame(
        [x[0][2]['score'] if x[0] and len(x[0]) > 2 else None for x in games_df[['team_1_linescores']].values])
    games_df['team_1_q4'] = pd.DataFrame(
        [x[0][3]['score'] if x[0] and len(x[0]) > 3 else None for x in games_df[['team_1_linescores']].values])
    games_df['team_1_ot'] = pd.DataFrame(
        [x[0][4]['score'] if x[0] and len(x[0]) > 4 else None for x in games_df[['team_1_linescores']].values])
    games_df['team_2_q1'] = pd.DataFrame(
        [x[0][0]['score'] if x[0] else None for x in games_df[['team_2_linescores']].values])
    games_df['team_2_q2'] = pd.DataFrame(
        [x[0][1]['score'] if x[0] and len(x[0]) > 1 else None for x in games_df[['team_2_linescores']].values])
    games_df['team_2_q3'] = pd.DataFrame(
        [x[0][2]['score'] if x[0] and len(x[0]) > 2 else None for x in games_df[['team_2_linescores']].values])
    games_df['team_2_q4'] = pd.DataFrame(
        [x[0][3]['score'] if x[0] and len(x[0]) > 3 else None for x in games_df[['team_2_linescores']].values])
    games_df['team_2_ot'] = pd.DataFrame(
        [x[0][4]['score'] if x[0] and len(x[0]) > 4 else None for x in games_df[['team_2_linescores']].values])
    games_df.drop(["team_1_linescores", "team_2_linescores"], axis=1, inplace=True)
    fixup(games_df)
    # Sort by game_id (note this is no same as datetime due to starting with modern games and then going back to older ones in API game_id naming)
    games_df.sort_values(by="game_id", inplace=True)
    store_helper.add_missing_columns(games_df, store_columns.GAMES)
    store_helper.ensure_type_columns(games_df, store_columns.GAMES)
    store_helper.reorder_columns(games_df, store_columns.GAMES)
    return games_df


def fixup(games_df: pd.DataFrame):
    """
    Helper for cleaning just related to messy values after already in nice columns
    :param games_df: The dataframe to fix
    :return:  The cleaned dataframe
    """
    # Fix busted game identifier for 2004 pre-season game
    games_df.loc[games_df['game_id'] == 1104, 'date_start'] = "2004-06-10T20:30:00-05:00"
    games_df.loc[games_df['game_id'] == 1104, 'year'] = 2004
    # Column filled with garbage
    games_df['game_duration'] = np.nan
    # Unnecessary
    games_df['tickets_url'] = np.nan
    # Had attendance 1
    games_df.loc[games_df['game_id'] == 1901, 'attendance'] = np.nan
    games_df.loc[games_df['attendance'] == 0, 'attendance'] = np.nan
    # For some reason not all older games were set as complete
    # Note this is set up so that 2021 cancelled games remain with that indicator
    games_df.loc[(games_df['year'] != 2022) & (
            (games_df['event_status_id'] == 4) | (games_df['event_status_id'] == 2)), 'event_status'] = "Final"
    games_df.loc[(games_df['year'] != 2022) & (
            (games_df['event_status_id'] == 1) | (games_df['event_status_id'] == 2)), 'event_status_id'] = 4
    # Make in-active games not show a status
    games_df.loc[~games_df['is_active'], 'quarter'] = np.nan
    games_df.loc[~games_df['is_active'], 'minutes'] = np.nan
    games_df.loc[~games_df['is_active'], 'seconds'] = np.nan
    games_df.loc[~games_df['is_active'], 'down'] = np.nan
    games_df.loc[~games_df['is_active'], 'yards_to_go'] = np.nan
    # These are un-played cancelled or current year games and this team_1 is always the road
    games_df.loc[games_df['team_1_is_at_home'] == True, 'team_1_is_at_home'] = False
