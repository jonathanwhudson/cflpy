import numpy as np
import pandas as pd

import column_dtypes
import helper
import load
import parse_advanced_boxscore


def parse_advanced_games(dataframe: pd.DataFrame) -> dict:
    result = {}
    result['pbp'] = parse_pbp(dataframe)
    result['penalties'] = parse_penalties(dataframe)
    result['reviews'] = parse_reviews(dataframe)
    result['roster'] = parse_roster(dataframe)
    result['boxscore'] = parse_advanced_boxscore.parse_boxscore(dataframe)
    return result


def parse_pbp(dataframe: pd.DataFrame) -> pd.DataFrame:
    pbp_df = helper.unroll_2(dataframe, 'game_id', 'play_by_play')
    pbp_df = helper.flatten(pbp_df, 'play_by_play')
    positions = ['quarterback', 'ball_carrier', 'primary_defender']
    parts = ['first_name', 'middle_name', 'last_name', 'birth_date', 'cfl_central_id']
    for pos in positions:
        for part in parts:
            pbp_df[f"{pos}_{part}"] = pd.DataFrame(
                [x[0][pos][part] if x else None for x in pbp_df[['players']].values])
    pbp_df.drop(['players'], axis=1, inplace=True)
    for pos in positions:
        pbp_df[f"{pos}_birth_date"] = pd.to_datetime(pbp_df[f"{pos}_birth_date"], format='%Y-%m-%d')
    # Some play ids are double to enter for info, set this up as new column
    pbp_df['entry'] = 0
    duplicates = pbp_df.loc[pbp_df.duplicated(subset=['play_id'], keep=False)]
    duplicates = duplicates.sort_values(by=['game_id', 'play_sequence', 'play_success_description'])
    entry = 1
    play_id = None
    for index, row in duplicates.iterrows():
        if play_id is not None:
            if play_id != row['play_id']:
                entry = 1
        play_id = row['play_id']
        pbp_df.at[index, 'entry'] = entry
        entry += 1
    # Fix up rest of data
    fixup_pbp(pbp_df)

    helper.ensure_column_type(pbp_df, column_dtypes.GAMES_ADV_PBP)
    return pbp_df


def fixup_pbp(pbp_df: pd.DataFrame):
    # Fixup the play clocks for plays where it isn't valid
    pbp_df.loc[
        (pbp_df['play_clock_start'] == "") & (pbp_df['play_type_id'] != 0), 'play_clock_start'] = "00:00"
    pbp_df.loc[
        (pbp_df['play_clock_start'] == "") & (pbp_df['play_type_id'] == 0), ['play_clock_start_in_secs',
                                                                             'play_clock_start',
                                                                             'field_position_start',
                                                                             'field_position_end', 'down',
                                                                             'yards_to_go',
                                                                             'is_in_red_zone']] = np.nan
    # One odd parsing issue
    pbp_df.loc[(pbp_df['play_id'] == 122546), 'play_clock_start'] = '4:28'
    pbp_df.loc[(pbp_df['play_id'] == 122546), 'play_clock_start_in_secs'] = 4 * 60 + 28
    # Same idea as previous but for field position
    pbp_df.loc[(pbp_df['field_position_start'] != "") &
               (pbp_df['field_position_start'].str.startswith('0')), ['field_position_start']] = "O" + pbp_df[
                                                                                                           'field_position_start'].str[
                                                                                                       1:]

    pbp_df.loc[pbp_df['field_position_end'] == "", 'field_position_end'] = np.nan
    pbp_df.loc[pbp_df['field_position_start'] == "55", 'field_position_start'] = "C55"
    pbp_df.loc[(pbp_df['field_position_start'] != ""), 'field_position_start'] = pbp_df[
        'field_position_start'].str.replace("-", "")
    # pbp_df.loc[pbp_df['field_position_start'] == "H-12", 'field_position_start'] = "H12"

    # End still dirty up to 2015 with random team locations missing (Don't necessarily need for ep/wp
    pbp_df.loc[pbp_df['field_position_end'] == "55", 'field_position_end'] = "C55"
    pbp_df.loc[(pbp_df['field_position_end'] != "") &
               (pbp_df['field_position_end'].str.startswith('0')), ['field_position_end']] = "O" + pbp_df[
                                                                                                       'field_position_end'].str[
                                                                                                   1:]
    pbp_df.loc[(pbp_df['field_position_end'] != ""), 'field_position_end'] = pbp_df['field_position_end'].str.replace(
        "-", "")

    pbp_df.loc[pbp_df['field_position_start'] == "", ['field_position_start', 'field_position_end']] = np.nan

    # Fix up one odd down
    pbp_df.loc[pbp_df['down'] == 11, 'down'] = 1
    # Fixup downs which were 0 to nan
    pbp_df.loc[pbp_df['down'] == 0, ['down', 'yards_to_go']] = np.nan


def parse_penalties(dataframe: pd.DataFrame) -> pd.DataFrame:
    # Do some rename shenanigans because there's a duplicate game_id in each penalty
    penalties_df = dataframe.rename(columns={'game_id': 'game_id_temp'})
    penalties_df = helper.unroll_2(penalties_df, 'game_id_temp', 'penalties')
    penalties_df = helper.flatten(penalties_df, 'penalties')
    penalties_df.drop(['game_id'], axis=1, inplace=True)
    penalties_df.rename(columns={'game_id_temp': 'game_id'}, inplace=True)
    fixup_penalties(penalties_df)
    helper.ensure_column_type(penalties_df, column_dtypes.GAMES_ADV_PEN)
    return penalties_df


def fixup_penalties(penalties_df: pd.DataFrame):
    # Penalties not given to a player should null out those entries
    penalties_df.loc[
        penalties_df['cfl_central_id'] == 0, ['cfl_central_id', 'first_name', 'middle_name', 'last_name']] = np.nan


def parse_reviews(dataframe: pd.DataFrame) -> pd.DataFrame:
    # Do some rename shenanigans because there's a duplicate game_id in each review
    reviews_df = dataframe.rename(columns={'game_id': 'game_id_temp'})
    reviews_df = helper.unroll_2(reviews_df, 'game_id_temp', 'play_reviews')
    reviews_df = helper.flatten(reviews_df, 'play_reviews')
    reviews_df.drop(['game_id'], axis=1, inplace=True)
    reviews_df.rename(columns={'game_id_temp': 'game_id'}, inplace=True)
    helper.ensure_column_type(reviews_df, column_dtypes.GAMES_ADV_REV)
    return reviews_df


def parse_roster(dataframe: pd.DataFrame) -> pd.DataFrame:
    rosters = pd.concat([pd.DataFrame(dataframe['game_id']), pd.DataFrame(dataframe['rosters'].values.tolist())],
                        axis=1)
    team_1_rosters = pd.DataFrame([[a, b['team_1']] for a, b in rosters[['game_id', 'teams']].values],
                                  columns=['game_id', 'temp'])
    team_1_rosters = helper.flatten(team_1_rosters, "temp")
    team_1_rosters = helper.unroll_4(team_1_rosters, ['game_id', 'abbreviation', 'team_id'], 'roster')
    team_1_rosters = helper.flatten(team_1_rosters, "roster")
    team_2_rosters = pd.DataFrame([[a, b['team_2']] for a, b in rosters[['game_id', 'teams']].values],
                                  columns=['game_id', 'temp'])
    team_2_rosters = helper.flatten(team_2_rosters, "temp")
    team_2_rosters = helper.unroll_4(team_2_rosters, ['game_id', 'abbreviation', 'team_id'], 'roster')
    team_2_rosters = helper.flatten(team_2_rosters, "roster")
    roster_df = pd.concat([team_1_rosters, team_2_rosters])
    fixup_roster(roster_df)
    helper.ensure_column_type(roster_df, column_dtypes.GAMES_ADV_ROS)
    return roster_df


def fixup_roster(roster_df: pd.DataFrame):
    # Clearly invalid birthdays should not be here
    roster_df.loc[roster_df['birth_date'].str[:4] == '1900', 'birth_date'] = np.nan


if __name__ == '__main__':
    from load import *

    games_df = load_games_basic_parsed(config.YEAR_START_ADV_USEFUL, config.YEAR_END_GAMES)
    games = load.extract_year_game_id_pairs_active(games_df)
    games_adv_df = load_games_advanced(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV, games)
    result = parse_advanced_games(games_adv_df)
    print("Done")
