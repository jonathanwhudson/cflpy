import pandas as pd

from helper import ensure_column_type, flatten


def dataframe_games(games: list[dict]) -> pd.DataFrame:
    games_df = pd.DataFrame(games)
    # Fix busted game identifier for 2004 pre-season game
    games_df.loc[games_df['game_id'] == 1104, ['date_start']] = "2004-06-10T20:30:00-05:00"
    # Set as datetime ISO 8601 format with timezone in UTC
    games_df['date_start'] = pd.to_datetime(games_df['date_start'], format='%Y-%m-%dT%H:%M:%S%Z', utc=True)
    # Pull year as it is so common of filter
    games_df['year'] = games_df['date_start'].dt.year
    # Handle event type (also has possible title for game)
    games_df = flatten(games_df, "event_type")
    games_df.rename(columns={'name': 'event_type', "title": "event_title"}, inplace=True)
    # Handle event status
    games_df = flatten(games_df, "event_status")
    games_df.rename(columns={'name': 'event_status'}, inplace=True)
    # Handle venue
    games_df = flatten(games_df, "venue")
    games_df.rename(columns={'name': 'venue'}, inplace=True)
    # Handle weather
    games_df = flatten(games_df, "weather")
    # Handle coin toss
    games_df = flatten(games_df, "coin_toss")
    # Start with team information flattening
    games_df = pd.concat([games_df, pd.DataFrame(games_df['team_1'].values.tolist()).add_prefix("team_1_")], axis=1)
    games_df = pd.concat([games_df, pd.DataFrame(games_df['team_2'].values.tolist()).add_prefix("team_2_")], axis=1)
    games_df = games_df.drop(["team_1", "team_2"], axis=1)
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
    games_df = games_df.drop(["team_1_linescores", "team_2_linescores"], axis=1)
    ensure_column_type(games_df,
                              {'game_id': 'int', 'year': 'int', 'date_start': 'datetime64[ns, UTC]', 'season': 'int',
                               'week': 'int', 'game_number': 'int', 'game_duration': 'Int64', 'attendance': 'int',
                               'event_type_id': 'int', 'event_type': 'string', 'event_title': 'string',
                               'venue_id': 'int', 'venue': 'string', 'event_status_id': 'int', 'event_status': 'string',
                               'is_active': 'bool', 'quarter': 'int', 'minutes': 'Int64', 'seconds': 'Int64',
                               'down': 'Int64', 'yards_to_go': 'Int64', 'team_1_team_id': 'int',
                               'team_1_location': 'string', 'team_1_nickname': 'string',
                               'team_1_abbreviation': 'string', 'team_1_score': 'int', 'team_1_venue_id': 'int',
                               'team_1_is_at_home': 'boolean', 'team_1_is_winner': 'boolean', 'team_2_team_id': 'int',
                               'team_2_location': 'string', 'team_2_nickname': 'string',
                               'team_2_abbreviation': 'string', 'team_2_score': 'int', 'team_2_venue_id': 'int',
                               'team_2_is_at_home': 'boolean', 'team_2_is_winner': 'boolean', 'team_1_q1': 'Int64',
                               'team_1_q2': 'Int64', 'team_1_q3': 'Int64', 'team_1_q4': 'Int64', 'team_1_ot': 'Int64',
                               'team_2_q1': 'Int64', 'team_2_q2': 'Int64', 'team_2_q3': 'Int64', 'team_2_q4': 'Int64',
                               'team_2_ot': 'Int64', 'coin_toss_winner': 'string',
                               'coin_toss_winner_election': 'string', 'temperature': 'int', 'sky': 'string',
                               'wind_speed': 'string', 'wind_direction': 'string', 'field_conditions': 'string',
                               'tickets_url': 'string'})
    # Sort by game_id (note this is no same as datetime due to starting with modern games and then going back to older ones in API game_id naming)
    games_df = games_df.sort_values(by="game_id")
    return games_df
