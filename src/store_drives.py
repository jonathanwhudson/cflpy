import sqlite3

import numpy as np
import pandas as pd

import column_dtypes
import config
import helper
import store


def load_drives_year_range(start: int, end: int, limit_years: list[int] = None) -> pd.DataFrame:
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not loading any drives as nothing in years=[{start},{end}]!")
    if limit_years:
        limit_years = set(limit_years)
        years = years.intersection(limit_years)
        if not years:
            raise ValueError(
                f"Not loading any drives as nothing in years=[{start},{end}] due to limit_years={limit_years}!")
    drives_df = load_drives(list(years), game_ids=None)
    return helper.clean_and_order_columns(drives_df, column_dtypes.DRIVES_COLUMNS)


def load_drives_year(year: int) -> pd.DataFrame:
    if year == 2020:
        raise ValueError(f"Not loading drives as year={year} did not get played!")
    drives_df = load_drives([year], game_ids=None)
    return helper.clean_and_order_columns(drives_df, column_dtypes.DRIVES_COLUMNS)


def load_drives_games(game_ids: list[int]) -> pd.DataFrame:
    drives_df = load_drives(years=None, game_ids=game_ids)
    return helper.clean_and_order_columns(drives_df, column_dtypes.DRIVES_COLUMNS)


def load_drives_game(game_id: int) -> pd.DataFrame:
    drives_df = load_drives(years=None, game_ids=[game_id])
    return helper.clean_and_order_columns(drives_df, column_dtypes.DRIVES_COLUMNS)


def remove_drives_year_range(start: int, end: int, limit_years: list[int] = None) -> None:
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not removing any drives as nothing in years=[{start},{end}]!")
    if limit_years:
        limit_years = set(limit_years)
        years = limit_years.intersection(years)
        if years:
            raise ValueError(
                f"Not removing any drives as nothing in years=[{start},{end}] due to limit_years={limit_years}!")
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(
        f"""SELECT DISTINCT(game_id) FROM drives WHERE year in {str(tuple(years)).replace(",)", ")")}""",
        connection)
    connection.close()
    game_ids = dataframe['game_id'].values.tolist()
    remove_drives_games(game_ids)


def remove_drives_year(year: int) -> None:
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(f"SELECT DISTINCT(game_id) FROM drives WHERE year={year}", connection)
    game_ids = dataframe['game_id'].values.tolist()
    connection.close()
    remove_drives_games(game_ids)


def remove_drives_games(game_ids: list[int]):
    game_ids = set(game_ids)
    connection = sqlite3.connect(config.DB_FILE)
    connection.execute(f'''DELETE FROM drives WHERE game_id in {str(tuple(game_ids)).replace(",)", ")")}''')
    connection.commit()
    connection.close()


def remove_drives_game(game_id: int):
    remove_drives_games([game_id])


def load_drives(years: list[int] = None, game_ids: list[int] = None) -> pd.DataFrame:
    if not years and not game_ids:
        raise ValueError(f"Not loading any drives as no year or game_ids were given!")
    add = ""
    if years:
        years = set(years)
        add += f"games.year in {str(tuple(years)).replace(',)', ')')}"
    if years and game_ids:
        add += " AND "
    if game_ids:
        game_ids = set(game_ids)
        add += f"""games.game_id in {str(tuple(game_ids)).replace(",)", ")")}"""
    query = f"""SELECT pbp.*,games.year,games.team_1_team_id, games.team_1_score, games.team_1_is_at_home, games.team_1_is_winner,
         games.team_2_team_id, games.team_2_abbreviation, games.team_2_score, games.team_2_is_at_home, games.team_2_is_winner FROM pbp LEFT JOIN games ON pbp.game_id=games.game_id WHERE {add};"""
    pbp_df = store.query(query)
    # Store if game reached OT or not
    pbp_df['ot'] = pbp_df['quarter'] == 5

    # Change field position start to distance from scoring TD
    pbp_df.loc[(pbp_df['field_position_start'] is not None) & (
            pbp_df['field_position_start'].str[0] == pbp_df['team_abbreviation'].str[0]), 'distance'] = 110 - pbp_df[
                                                                                                                  'field_position_start'].str[
                                                                                                              1:].astype(
        'Int64')
    pbp_df.loc[(pbp_df['field_position_start'] is not None) & (
            pbp_df['field_position_start'].str[0] != pbp_df['team_abbreviation'].str[0]), 'distance'] = pbp_df[
                                                                                                            'field_position_start'].str[
                                                                                                        1:].astype(
        'Int64')
    pbp_df['distance'] = pbp_df['distance'].astype('Int64')

    # Get a time remaining in game
    pbp_df.loc[~pbp_df['ot'], 'time_remaining'] = (4 - pbp_df['quarter']) * 900 + pbp_df['play_clock_start_in_secs']
    pbp_df.loc[pbp_df['ot'], 'time_remaining'] = 0
    pbp_df['time_remaining'] = pbp_df['time_remaining'].astype('Int64')

    # Determine if team with ball is home team (this helps with pulling score of team with ball)
    pbp_df.loc[pbp_df['team_1_team_id'] == pbp_df['team_id'], 'won'] = pbp_df['team_1_is_winner']
    pbp_df.loc[pbp_df['team_2_team_id'] == pbp_df['team_id'], 'won'] = pbp_df['team_2_is_winner']
    pbp_df['won'] = pbp_df['won'].astype('bool')
    pbp_df.loc[pbp_df['team_1_team_id'] == pbp_df['team_id'], 'home'] = pbp_df['team_1_is_at_home']
    pbp_df.loc[pbp_df['team_2_team_id'] == pbp_df['team_id'], 'home'] = pbp_df['team_2_is_at_home']
    pbp_df['home'] = pbp_df['home'].astype('bool')

    # Calculate the score differential in game
    pbp_df.loc[pbp_df['home'], 'score_diff'] = pbp_df['team_home_score'] - pbp_df['team_visitor_score']
    pbp_df.loc[~pbp_df['home'], 'score_diff'] = pbp_df['team_visitor_score'] - pbp_df['team_home_score']
    pbp_df['score_diff'] = pbp_df['score_diff'].astype('int')

    # Calculate the total score in game at point
    pbp_df.loc[pbp_df['home'], 'total_score'] = pbp_df['team_home_score'] + pbp_df['team_visitor_score']
    pbp_df.loc[~pbp_df['home'], 'total_score'] = pbp_df['team_visitor_score'] + pbp_df['team_home_score']
    pbp_df['total_score'] = pbp_df['total_score'].astype('int')

    # Adjust score diff for time remaining in game
    pbp_df['score_diff_calc'] = pbp_df['score_diff'] / np.sqrt(pbp_df['time_remaining'] + 1)
    pbp_df['score_diff_calc'] = pbp_df['score_diff_calc'].astype('float')

    # Determine what type of play it is
    pbp_df.loc[pbp_df['play_type_id'] == 4, 'kickoff'] = True
    pbp_df.loc[pbp_df['play_type_id'] == 4, 'down'] = 1
    pbp_df.loc[pbp_df['play_type_id'] == 4, 'yards_to_go'] = 10
    pbp_df.loc[pbp_df['play_type_id'] != 4, 'kickoff'] = False
    pbp_df['kickoff'] = pbp_df['kickoff'].astype('bool')

    pbp_df.loc[pbp_df['play_type_id'] == 3, 'conv1'] = True
    pbp_df.loc[pbp_df['play_type_id'] != 3, 'conv1'] = False
    pbp_df['conv1'] = pbp_df['conv1'].astype('bool')

    pbp_df.loc[pbp_df['play_type_id'] == 8, 'conv2'] = True
    pbp_df.loc[pbp_df['play_type_id'] == 9, 'conv2'] = True
    pbp_df.loc[(pbp_df['play_type_id'] != 8) & (pbp_df['play_type_id'] != 9), 'conv2'] = False
    pbp_df['conv2'] = pbp_df['conv2'].astype('bool')

    pbp_df.loc[~pbp_df['kickoff'] & ~pbp_df['conv1'] & ~pbp_df['conv2'], 'regular'] = True
    pbp_df.loc[pbp_df['kickoff'] | pbp_df['conv1'] | pbp_df['conv2'], 'regular'] = False
    pbp_df['regular'] = pbp_df['regular'].astype('bool')

    # Finally determine if points were scored on the drive
    # Start with NaN entries
    pbp_df['drive_id'] = np.nan
    pbp_df['drive_sequence'] = np.nan
    pbp_df['points_scored'] = np.nan
    # Sort the plays
    pbp_df.sort_values(by=['game_id', 'play_sequence', 'entry'], inplace=True)
    # Start at 1 for all drives, and for a games drives
    drive_id = 1
    drive_sequence = 1
    # So we notice unique games to reset sequence
    game_id = None
    # Used to help us determine if points were scored consistently
    home_score = None
    visitor_score = None
    # Loop through plays
    for index, row in pbp_df.iterrows():
        # If we are on a new game then reset things (except global drive_id)
        if game_id is None or row['game_id'] != game_id:
            drive_sequence = 1
            game_id = row['game_id']
            home_score = None
            visitor_score = None
        # All plays are default included in a drive (we'll then determine if next play will be a new drive)
        # Note this will include plays like timeouts,penalties,end quarter/game etc. that aren't 'real' plays to a drive
        pbp_df.at[index, 'drive_id'] = drive_id
        pbp_df.at[index, 'drive_sequence'] = drive_sequence
        last_play = False
        # Gave up a safety against (applies well to second of two entry plays)
        if row['play_success_id'] == 24:
            last_play = True
        # Gave up a touchdown due to fumble (applies well to second of two entry plays)
        elif row['play_success_id'] == 110:
            last_play = True
        # Gave up a touchdown due to missed field goal return
        elif row['play_success_id'] == 16:
            last_play = True
        # Kickoff completed
        elif row['play_success_id'] == 29:
            last_play = True
        # Success field goal
        elif row['play_success_id'] == 10:
            last_play = True
        # Success punt
        elif row['play_success_id'] == 58:
            last_play = True
        # Success 1pt convert
        elif row['play_success_id'] == 3:
            last_play = True
        # Success 2pt convert
        elif row['play_success_id'] == 6:
            last_play = True
        # Failed 2pt pass
        elif row['play_success_id'] == 903:
            last_play = True
        # Failed 2pt rush
        elif row['play_success_id'] == 902:
            last_play = True
        # Failed 1pt kick
        elif row['play_success_id'] == 901:
            last_play = True
        # Failed 1pt kick blocked
        elif row['play_success_id'] == 116:
            last_play = True
        # Failed 1pt kick missed
        elif row['play_success_id'] == 111:
            last_play = True
        # Failed FG single
        elif row['play_success_id'] == 13:
            last_play = True
        # Failed FG turnover
        elif row['play_success_id'] == 11:
            last_play = True
        # Safety result that isn't first of two entries
        elif row['play_result_type_id'] == 10 and row['entry'] == 0:
            last_play = True
        # Change of possessions due to field goal, punt, downs, missed fg, int, fumble, end of half, end of game
        elif row['play_result_type_id'] in [2, 3, 4, 5, 6, 7, 8, 9]:
            last_play = True
        # TD Scored
        elif row['play_result_type_id'] == 1 and row['play_result_points'] == 6:
            last_play = True
        # Sacked on 2pt convert after TD
        elif row['play_result_type_id'] == 1 and row['play_success_id'] == 78:
            last_play = True
        pbp_df.at[index, 'last_play'] = last_play
        if last_play:
            drive_id += 1
            drive_sequence += 1
        if home_score is None or visitor_score is None:
            home_score = row['team_home_score']
            visitor_score = row['team_visitor_score']
            if home_score != 0 or visitor_score != 0:
                if row['home']:
                    pbp_df.at[index, 'points_scored'] = home_score - visitor_score
                else:
                    pbp_df.at[index, 'points_scored'] = visitor_score - home_score
            else:
                pbp_df.at[index, 'points_scored'] = 0
        else:
            if row['home']:
                pbp_df.at[index, 'points_scored'] = (row['team_home_score'] - home_score) - (
                        row['team_visitor_score'] - visitor_score)
            else:
                pbp_df.at[index, 'points_scored'] = (row['team_visitor_score'] - visitor_score) - (
                        row['team_home_score'] - home_score)
            home_score = row['team_home_score']
            visitor_score = row['team_visitor_score']

    del drive_id, drive_sequence, game_id, home_score, visitor_score, index, last_play, row

    pbp_df['last_play'] = pbp_df['last_play'].astype('bool')
    pbp_df['drive_id'] = pbp_df['drive_id'].astype('int')
    pbp_df['drive_sequence'] = pbp_df['drive_sequence'].astype('int')
    pbp_df['points_scored'] = pbp_df['points_scored'].astype('int')
    grouped = \
        pbp_df[['game_id', 'drive_sequence', 'points_scored']].groupby(by=['game_id', 'drive_sequence']).apply(sum)[
            ['points_scored']]
    grouped = grouped.reset_index()

    pbp_df = pbp_df.merge(grouped, how='left', left_on=['game_id', 'drive_sequence'],
                          right_on=['game_id', 'drive_sequence'])
    del grouped
    pbp_df.rename(columns={"points_scored_x": "points_scored", "points_scored_y": "points_scored_on_drive"},
                  inplace=True)
    pbp_df['points_scored_on_drive'] = pbp_df['points_scored_on_drive'].astype('int')

    drives_df = pbp_df[
        ['year', 'game_id', 'play_id', 'play_sequence', 'entry', "drive_id", "drive_sequence", "home", "won", "kickoff",
         "conv1", "conv2", "regular", "ot", "quarter", "time_remaining", "down", "yards_to_go",
         "distance", "score_diff", "score_diff_calc", "total_score",
         "last_play", "points_scored", "points_scored_on_drive"]]
    return drives_df


def store_drives(drives_df: pd.DataFrame, if_exists: str = store.IF_EXISTS_REPLACE):
    store.store_dataframe(drives_df, "drives", if_exists=if_exists)


def reset_drives_all() -> None:
    drives_df = load_drives_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    remove_drives_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    store_drives(drives_df, store.IF_EXISTS_REPLACE)


def reset_drives_year(year: int) -> None:
    drives_df = load_drives_year(year)
    remove_drives_year(year)
    store_drives(drives_df, store.IF_EXISTS_APPEND)


def reset_drives_current_year() -> None:
    reset_drives_year(config.YEAR_CURRENT)


def reset_drives_games(game_ids: list[int]) -> None:
    drives_df = load_drives_games(game_ids)
    remove_drives_games(game_ids)
    store_drives(drives_df, store.IF_EXISTS_APPEND)


def reset_drives_game(game_id: int) -> None:
    drives_df = load_drives_game(game_id)
    remove_drives_game(game_id)
    store_drives(drives_df, store.IF_EXISTS_APPEND)


def main() -> None:
    # Reset all drives (note that this should be followed by EPA/GEI and others that require drive info)
    if False:
        print("Resetting all drives")
        reset_drives_all()
    # Reset all drives for current year (note that this should be followed by EPA/GEI and others that require drive info)
    if True:
        print("Resetting drives for current year")
        reset_drives_current_year()
    # Reset all drives for chosen year (note that this should be followed by EPA/GEI and others that require drive info)
    if False:
        year = 2022
        print(f"Resetting drives for year=<{year}>")
        reset_drives_year(year)
    # Reset all drives for certain game(note that this should be followed by EPA/GEI and others that require drive info)
    if False:
        game_ids = [6226, 6227, 6228]
        print(f"Resetting drives for games=<{game_ids}>")
        reset_drives_games(game_ids)
    # Reset all drives for certain game(note that this should be followed by EPA/GEI and others that require drive info)
    if False:
        game_id = 6228
        print(f"Resetting drives for game=<{game_id}>")
        reset_drives_game(game_id)


if __name__ == '__main__':
    main()
    print("Done")
