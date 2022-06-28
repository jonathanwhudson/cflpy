import pandas as pd

from helper import ensure_column_type, flatten


def dataframe_playbyplay(data: list[dict]) -> pd.DataFrame:
    dataframe = pd.DataFrame(data)
    playbyplay_df = pd.DataFrame([[a, c] for a, b in dataframe[['game_id', 'play_by_play']].values for c in b],
                                 columns=['game_id', 'play'])
    playbyplay_df = flatten(playbyplay_df, 'play')
    positions = ['quarterback', 'ball_carrier', 'primary_defender']
    parts = ['first_name', 'middle_name', 'last_name', 'birth_date', 'cfl_central_id']
    for pos in positions:
        for part in parts:
            playbyplay_df[f"{pos}_{part}"] = pd.DataFrame(
                [x[0][pos][part] if x else None for x in playbyplay_df[['players']].values])
    playbyplay_df = playbyplay_df.drop(['players'], axis=1)
    for pos in positions:
        playbyplay_df[f"{pos}_birth_date"] = pd.to_datetime(playbyplay_df[f"{pos}_birth_date"], format='%Y-%m-%d')
    ensure_column_type(playbyplay_df, {'game_id': 'int', 'play_id': 'int', 'play_sequence': 'int', 'quarter': 'int',
                                       'play_clock_start': 'string', 'play_clock_start_in_secs': 'int',
                                       'field_position_start': 'string', 'field_position_end': 'string', 'down': 'int',
                                       'yards_to_go': 'int', 'is_in_red_zone': 'bool', 'team_home_score': 'int',
                                       'team_visitor_score': 'int', 'play_type_id': 'int',
                                       'play_type_description': 'string',
                                       'play_result_type_id': 'int', 'play_result_type_description': 'string',
                                       'play_result_yards': 'int', 'play_result_points': 'int',
                                       'play_success_id': 'int',
                                       'play_success_description': 'string',
                                       'play_change_of_possession_occurred': 'bool',
                                       'team_abbreviation': 'string', 'team_id': 'int', 'play_summary': 'string',
                                       'quarterback_cfl_central_id': 'int', 'ball_carrier_cfl_central_id': 'int',
                                       'primary_defender_cfl_central_id': 'int',
                                       'quarterback_first_name': 'string', 'ball_carrier_first_name': 'string',
                                       'primary_defender_first_name': 'string', 'quarterback_middle_name': 'string',
                                       'ball_carrier_middle_name': 'string', 'primary_defender_middle_name': 'string',
                                       'quarterback_last_name': 'string', 'ball_carrier_last_name': 'string',
                                       'primary_defender_last_name': 'string',
                                       'quarterback_birth_date': 'datetime64[ns]',
                                       'ball_carrier_birth_date': 'datetime64[ns]',
                                       'primary_defender_birth_date': 'datetime64[ns]'})
    return playbyplay_df


def dataframe_penalties(data: list[dict]) -> pd.DataFrame:
    dataframe = pd.DataFrame(data)
    penalties_df = pd.DataFrame([[a, c] for a, b in dataframe[['game_id', 'penalties']].values for c in b],
                                columns=['game_id_temp', 'penalty'])
    penalties_df = flatten(penalties_df, 'penalty')
    penalties_df = penalties_df.drop(['game_id'], axis=1)
    penalties_df.rename(columns={'game_id_temp': 'game_id'}, inplace=True)
    ensure_column_type(penalties_df, {'game_id': 'int', 'play_id': 'int', 'play_sequence': 'int', 'quarter': 'int',
                                      'play_clock_start': 'string', 'play_clock_start_in_secs': 'int',
                                      'play_summary': 'string', 'field_position_start': 'string',
                                      'field_position_end': 'string', 'down': 'int', 'yards_to_go': 'int',
                                      'penalty_id': 'int', 'penalty_code': 'string', 'penalty_name': 'string',
                                      'penalty_type_id': 'int', 'penalty_type_name': 'string',
                                      'penalty_situation_id': 'int',
                                      'penalty_situation_code': 'string', 'penalty_situation_name': 'string',
                                      'is_major': 'int', 'was_accepted': 'int', 'team_or_player_penalty': 'string',
                                      'team_abbreviation': 'string', 'team_id': 'int', 'cfl_central_id': 'int',
                                      'first_name': 'string', 'middle_name': 'string', 'last_name': 'string'})
    return penalties_df


def dataframe_reviews(data: list[dict]) -> pd.DataFrame:
    dataframe = pd.DataFrame(data)
    reviews_df = pd.DataFrame([[a, c] for a, b in dataframe[['game_id', 'play_reviews']].values for c in b],
                              columns=['game_id_temp', 'play_review'])
    reviews_df = flatten(reviews_df, 'play_review')
    reviews_df = reviews_df.drop(['game_id'], axis=1)
    reviews_df.rename(columns={'game_id_temp': 'game_id'}, inplace=True)
    ensure_column_type(reviews_df, {'game_id': 'int', 'play_id': 'int', 'quarter': 'int', 'play_clock_start': 'string',
                                    'play_clock_start_in_secs': 'int', 'play_summary': 'string',
                                    'field_position_start': 'string', 'field_position_end': 'string', 'down': 'int',
                                    'yards_to_go': 'int', 'play_type_id': 'int', 'play_type_description': 'string',
                                    'play_review_type_id': 'int', 'play_review_type_name': 'string',
                                    'play_reversed_on_review': 'bool'})
    return reviews_df


def dataframe_roster(data: list[dict]) -> pd.DataFrame:
    dataframe = pd.DataFrame(data)
    rosters = pd.concat([pd.DataFrame(dataframe['game_id']), pd.DataFrame(dataframe['rosters'].values.tolist())],
                        axis=1)
    team_1_rosters = pd.DataFrame([[a, b['team_1']] for a, b in rosters[['game_id', 'teams']].values],
                                  columns=['game_id', 'temp'])
    team_1_rosters = flatten(team_1_rosters, "temp")
    team_1_rosters = unroll_4(team_1_rosters, ['game_id', 'abbreviation', 'team_id'], 'roster')
    team_1_rosters = flatten(team_1_rosters, "roster")
    team_2_rosters = pd.DataFrame([[a, b['team_2']] for a, b in rosters[['game_id', 'teams']].values],
                                  columns=['game_id', 'temp'])
    team_2_rosters = flatten(team_2_rosters, "temp")
    team_2_rosters = unroll_4(team_2_rosters, ['game_id', 'abbreviation', 'team_id'], 'roster')
    team_2_rosters = flatten(team_2_rosters, "roster")
    roster_df = pd.concat([team_1_rosters, team_2_rosters])
    ensure_column_type(roster_df,
                       {'game_id': 'int', 'abbreviation': 'string', 'team_id': 'int', 'cfl_central_id': 'int',
                        'first_name': 'string', 'middle_name': 'string', 'last_name': 'string',
                        'birth_date': 'datetime64[ns]', 'uniform': 'int', 'position': 'string',
                        'is_national': 'bool', 'is_starter': 'bool', 'is_inactive': 'bool'})
    return roster_df


def unroll_4(dataframe: pd.DataFrame, keep: list[str], unroll: str) -> pd.DataFrame:
    columns = keep + [unroll]
    return pd.DataFrame([[a, b, c, e] for a, b, c, d in dataframe[columns].values for e in d], columns=columns)
