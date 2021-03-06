import ast

import numpy as np
import pandas as pd

from store import store_helper, store_columns
from store.parse import parse_helper


def parse_boxscore(dataframe: pd.DataFrame) -> dict[str:dict[str:pd.DataFrame]]:
    """
    Parse clean boxscore dataframes
    :param dataframe: Dataframe to extract if from
    :return: Cleaned dictionary of dataframes
    """
    result = {}
    teams = pd.concat([pd.DataFrame(dataframe['game_id']), pd.DataFrame(dataframe['boxscore'].values.tolist())], axis=1)
    team_1_boxscore = pd.DataFrame([[a, b['team_1']] for a, b in teams[['game_id', 'teams']].values],
                                   columns=['game_id', 'temp'])
    team_1_boxscore = pd.concat([team_1_boxscore, pd.DataFrame(team_1_boxscore['temp'].values.tolist())], axis=1)
    team_1_boxscore.drop(['temp'], axis=1, inplace=True)
    team_2_boxscore = pd.DataFrame([[a, b['team_2']] for a, b in teams[['game_id', 'teams']].values],
                                   columns=['game_id', 'temp'])
    team_2_boxscore = pd.concat([team_2_boxscore, pd.DataFrame(team_2_boxscore['temp'].values.tolist())], axis=1)
    team_2_boxscore.drop(['temp'], axis=1, inplace=True)
    box_score = pd.concat([team_1_boxscore, team_2_boxscore]).reset_index()

    result['team'] = team_boxscore(box_score)
    for column in ['offence', 'turnovers', 'passing', 'rushing', 'receiving', 'punts', 'punt_returns', 'kick_returns',
                   'field_goals', 'field_goal_returns', 'kicking', 'converts', 'defence', 'penalties']:
        if column in box_score:
            box_score.drop(column, axis=1, inplace=True)
    result['players'] = player_boxscore(box_score)
    return result


def team_boxscore(dataframe: pd.DataFrame) -> dict[str:pd.DataFrame]:
    """
    Parse clean team boxscore dataframes
    :param dataframe: Dataframe to extract if from
    :return: Cleaned dictionary of dataframes
    """
    result = {}
    for (key, column) in store_columns.ADV_BOX_TEAM.items():
        if key == 'offence' or key == 'converts':
            pass
        if key in dataframe.columns:
            result[key] = flatten_stat_team(dataframe, key)
        else:
            result[key] = pd.DataFrame(columns=list(store_columns.ADV_BOX_TEAM[key].keys()))
    result['offence'] = team_boxscore_offence(dataframe)
    result['converts'] = team_boxscore_converts(dataframe)
    # Fix a couple unique columns in data
    result['passing']['pass_completion_percentage'] = result['passing']['pass_completion_percentage'].str.rstrip(
        '%').astype(
        'float') / 100.0
    for (key, column) in store_columns.ADV_BOX_TEAM.items():
        store_helper.add_missing_columns(result[key], column)
        store_helper.ensure_type_columns(result[key], column)
        store_helper.reorder_columns(result[key], column)
    return result


def player_boxscore(dataframe: pd.DataFrame) -> dict[str:pd.DataFrame]:
    """
    Parse clean player boxscore dataframes
    :param dataframe: Dataframe to extract if from
    :return: Cleaned dictionary of dataframes
    """
    result = {}
    team_boxscore_players = parse_helper.flatten(dataframe, "players")
    for (key, column) in store_columns.ADV_BOX_PLAYER.items():
        if key in team_boxscore_players.columns:
            result[key] = flatten_player_stat(team_boxscore_players, key)
        else:
            result[key] = pd.DataFrame(columns=list(store_columns.ADV_BOX_PLAYER[key].keys()))
    # Fix a couple unique columns in data
    result['passing']['pass_completion_percentage'] = result['passing']['pass_completion_percentage'].str.rstrip(
        '%').astype('float') / 100.0
    result['field_goals']['field_goal_missed_list'] = "[" + result['field_goals']['field_goal_missed_list'] + "]"
    result['field_goals']['field_goal_missed_list'] = result['field_goals']['field_goal_missed_list'].apply(
        ast.literal_eval)
    for (key, column) in store_columns.ADV_BOX_PLAYER.items():
        store_helper.add_missing_columns(result[key], column)
        store_helper.ensure_type_columns(result[key], column)
        store_helper.reorder_columns(result[key], column)
    return result


def team_boxscore_offence(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Parse clean team offence boxscore dataframe
    :param dataframe: Dataframe to extract if from
    :return: Cleaned dataframe
    """
    dataframe = pd.concat([pd.DataFrame(dataframe[['game_id', 'abbreviation', 'team_id']]),
                           pd.DataFrame(dataframe['offence'].values.tolist())], axis=1)
    dataframe['down_1_attempts'] = pd.DataFrame(
        [{y['down']: y for y in x[0]}[1]['attempts'] if 1 in {y['down']: y for y in x[0]} else None if x[0] else None
         for x in dataframe[['downs']].values])
    dataframe['down_2_attempts'] = pd.DataFrame(
        [{y['down']: y for y in x[0]}[2]['attempts'] if 2 in {y['down']: y for y in x[0]} else None if x[0] else None
         for x in dataframe[['downs']].values])
    dataframe['down_3_attempts'] = pd.DataFrame(
        [{y['down']: y for y in x[0]}[3]['attempts'] if 3 in {y['down']: y for y in x[0]} else None if x[0] else None
         for x in dataframe[['downs']].values])
    dataframe['down_1_yards'] = pd.DataFrame(
        [{y['down']: y for y in x[0]}[1]['yards'] if 1 in {y['down']: y for y in x[0]} else None if x[0] else None for x
         in dataframe[['downs']].values])
    dataframe['down_2_yards'] = pd.DataFrame(
        [{y['down']: y for y in x[0]}[2]['yards'] if 2 in {y['down']: y for y in x[0]} else None if x[0] else None for x
         in dataframe[['downs']].values])
    dataframe['down_3_yards'] = pd.DataFrame(
        [{y['down']: y for y in x[0]}[3]['yards'] if 3 in {y['down']: y for y in x[0]} else None if x[0] else None for x
         in dataframe[['downs']].values])
    return dataframe.drop(['downs'], axis=1)


def team_boxscore_converts(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Parse clean team converts boxscore dataframe
    :param dataframe: Dataframe to extract if from
    :return: Cleaned dataframe
    """
    dataframe = flatten_dict(dataframe, 'converts')
    dataframe = pd.DataFrame([[a, b, c, d['attempts'] if not type(d) == float else None,
                               d['made'] if not type(d) == float else None,
                               e['attempts'] if not type(e) == float else None,
                               e['made'] if not type(e) == float else None] for a, b, c, d, e, in
                              dataframe[
                                  ['game_id', 'abbreviation', 'team_id', 'one_point_converts',
                                   'two_point_converts']].values],
                             columns=['game_id', 'abbreviation', 'team_id', 'one_point_converts_attempts',
                                      'one_point_converts_made', 'two_point_converts_attempts',
                                      'two_point_converts_made'])
    return dataframe


def flatten_stat_team(dataframe: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Unique flatten for statistics for team
    :param dataframe: The dataframe to flatten in
    :param column: The column to flatten
    :return: The dataframe modified to be flattened
    """
    team_dataframe = dataframe[['game_id', 'abbreviation', 'team_id'] + [column]]
    team_dataframe = flatten_dict(team_dataframe, column)
    return team_dataframe


def flatten_player_stat(dataframe: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Unique flatten for statistics for player
    :param dataframe: The dataframe to flatten in
    :param column: The column to flatten
    :return: The dataframe modified to be flattened
    """
    player_dataframe = parse_helper.unroll_4(dataframe, ["game_id", 'abbreviation', 'team_id'], column)
    player_dataframe = parse_helper.flatten(player_dataframe, column)
    if not player_dataframe.empty:
        player_dataframe = parse_helper.flatten(player_dataframe, 'player')
    return player_dataframe


def flatten_dict(dataframe: pd.DataFrame, flatten_column: str) -> pd.DataFrame:
    """
    Flatten a dictionary columns
    :param dataframe: Data frame to flatten in
    :param flatten_column: The column to flatten
    :return: The dataframe modified to be flattened
    """
    return pd.concat([dataframe.loc[:, dataframe.columns != flatten_column],
                      pd.DataFrame(replace_nan_with_matching_nan_dict(dataframe[flatten_column]).values.tolist())],
                     axis=1)


def replace_nan_with_matching_nan_dict(series: pd.Series) -> pd.Series:
    """
    Series expected to be filled with same structure dictionary entries, and also nan
    Replace the nan with dictionaries of nan that match previous ones
    :param series: Series to make improved version of
    :return: Series with nan replaced by consistent dictionaries of nan
    """
    # putmask() only works on Index
    # We are using putmask() as Series/DataFrame options were treating the dict not as value, but a mapping
    index: pd.Index = pd.Index(series)
    # Find a value that is not a float (i.e. not nan -> assume a dict)
    dictionary = None
    for i in index.values:
        if not type(i) == float:
            dictionary = i.copy()
            break
    # If not found then just return
    if dictionary is None:
        return series
    # Now setup this dictionary copy to only store nan for each entry
    for key in dictionary:
        dictionary[key] = np.nan
    # Replace all nan with this dictionary of nan, return this
    index = index.putmask(index.isna(), dictionary)
    # Return as series again
    return pd.Series(index)
