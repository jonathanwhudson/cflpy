import numpy as np

import config
import store

def main():
    pbp_df = store.query(
        f"""SELECT pbp.*,games.year,games.team_1_team_id, games.team_1_score, games.team_1_is_at_home, games.team_1_is_winner,
         games.team_2_team_id, games.team_2_abbreviation, games.team_2_score, games.team_2_is_at_home, games.team_2_is_winner FROM pbp LEFT JOIN games ON pbp.game_id=games.game_id WHERE games.year >= {config.YEAR_START_ADV_USEFUL}""")
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
    # So we notice sack/X type plays with two entries
    play_id = None
    # Used to help us determine if points were scored consistently
    home_score = None
    visitor_score = None
    # Loop through plays
    for index, row in pbp_df.iterrows():
        # If we are on a new game then reset things (except global drive_id)
        if game_id is None or row['game_id'] != game_id:
            drive_sequence = 1
            game_id = row['game_id']
            play_id = None
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

    del drive_id, drive_sequence, game_id, play_id, home_score, visitor_score, index, last_play, row

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
        ['year', 'game_id', 'play_id','play_sequence','entry',
         "home", "drive_id", "drive_sequence", "points_scored", "last_play",
         "kickoff", "conv1", "conv2", "regular",
         "distance", "score_diff", "score_diff_calc", "total_score", "time_remaining", "down", "yards_to_go", "ot",
         "quarter", "points_scored_on_drive", "won"]]
    store.store_dataframe(drives_df, "drives", if_exists=store.IF_EXISTS_REPLACE)

if __name__ == '__main__':
    main()
    print("Done")
