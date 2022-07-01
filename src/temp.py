
import numpy as np

from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split

import sqlite3
import pandas as pd


def store_dataframe(df: pd.DataFrame, table_name: str, if_exists) -> None:
    connection = sqlite3.connect("data.db")
    df.to_sql(table_name, connection, if_exists=if_exists, index=False)
    connection.close()


def get_dataframe(table_name: str) -> pd.DataFrame:
    connection = sqlite3.connect("data.db")
    sql_query = pd.read_sql_query(f'''SELECT * FROM {table_name}''', connection)
    connection.close()
    return pd.DataFrame(sql_query)





def main() -> None:
    download_games_basic(reset=False)
    games = load_games_basic(YEAR_START_GAMES, YEAR_END_GAMES)
    print(games)
    return
    games_df = dataframe_games(games)
    download_games_advanced(games_df, YEAR_START_ADV, YEAR_END_ADV, reset=False)
    games_adv = load_games_advanced(games_df)
    pbp_df = dataframe_playbyplay(games_adv)
    pen_df = dataframe_penalties(games_adv)
    rev_df = dataframe_reviews(games_adv)
    ros_df = dataframe_roster(games_adv)
    result_dict_dfs = dataframe_boxscore(games_adv)
    store_dataframe(games_df, "games", 'replace')
    store_dataframe(pbp_df, "play_by_play", 'replace')
    store_dataframe(pen_df, "penalties", 'replace')
    store_dataframe(rev_df, "play_reviews", 'replace')
    store_dataframe(ros_df, "roster", 'replace')
    for type, value in result_dict_dfs.items():
        for name, df in value.items():
            store_dataframe(df, f"{type}_{name}", 'replace')


def games_needing_update():
    connection = sqlite3.connect("data.db")
    sql_query = pd.read_sql_query(
        f'''SELECT * FROM games WHERE year={YEAR_CURRENT} AND NOT event_status_id=4 AND date_start < CURRENT_DATE''',
        connection)
    sql_query = pd.read_sql_query(
        f'''SELECT * FROM games WHERE year={YEAR_CURRENT} AND event_type_id=1 AND week=3''',
        connection)
    #sql_query = pd.read_sql_query(
    #    f'''SELECT * FROM games WHERE year={YEAR_CURRENT} AND event_status_id=4 AND date_start < CURRENT_DATE AND event_type_id=1 AND week=3 and game_id=6222''',
    #    connection)
    connection.close()
    return pd.DataFrame(sql_query)


def remove_rows_game_id(table, game_ids):
    connection = sqlite3.connect("data.db")
    connection.execute(f'''DELETE FROM {table} WHERE game_id IN {tuple(game_ids)}''')
    connection.commit()
    connection.close()


def current() -> None:
    games_update_df = games_needing_update()
    print(games_update_df['game_id'])
    # download_games_advanced_year(games_update_df, YEAR_CURRENT, reset=True)
    download_games_advanced_year(games_update_df, YEAR_CURRENT, reset=False)
    games_adv = load_games_advanced(games_update_df)
    if games_adv:
        # download_games_basic_year(YEAR_CURRENT, reset=True)
        download_games_basic_year(YEAR_CURRENT, reset=False)
        games = load_games_basic_year(YEAR_CURRENT)
        games_df = dataframe_games(games)
        pbp_df = dataframe_playbyplay(games_adv)

        pbp_df['ot'] = pbp_df['quarter'] == 5
        #pbp_df['temp1'] = pbp_df['field_position_start'].str[0:0].isalpha()
        pbp_df['temp'] = pbp_df['field_position_start'].str[1:]
        print(pbp_df['temp'].isnull().values.any())
        pbp_df['temp'].replace([""], np.nan, inplace=True)
        pbp_df['temp'] = pbp_df['temp'].astype('Int64')
        print(pbp_df['temp'].isnull().values.any())
        pbp_df.loc[pbp_df['field_position_start'].str[0] == pbp_df['team_abbreviation'].str[0], 'distance'] = 110 - \
                                                                                                              pbp_df[
                                                                                                                  'temp']
        pbp_df.loc[pbp_df['field_position_start'].str[0] != pbp_df['team_abbreviation'].str[0], 'distance'] = pbp_df[
            'temp']
        print(pbp_df['distance'].isnull().values.any())
        pbp_df['distance'] = pbp_df['distance'].astype('Int64')

        pbp_df.loc[~pbp_df['ot'], 'time_remaining'] = (4 - pbp_df['quarter']) * 900 + pbp_df['play_clock_start_in_secs']
        pbp_df.loc[pbp_df['ot'], 'time_remaining'] = 0
        pbp_df['time_remaining'] = pbp_df['time_remaining'].astype('int')

        x = pbp_df.merge(games_df, how='left', left_on='game_id', right_on='game_id')[
            ['game_id', 'play_id', 'team_1_team_id', 'team_1_score', 'team_1_is_at_home', 'team_1_is_winner',
             'team_2_team_id', 'team_2_abbreviation', 'team_2_score', 'team_2_is_at_home', 'team_2_is_winner']]
        pbp_df.loc[x['team_1_team_id'] == pbp_df['team_id'], 'won'] = x['team_1_is_winner']
        pbp_df.loc[x['team_2_team_id'] == pbp_df['team_id'], 'won'] = x['team_2_is_winner']
        pbp_df['won'] = pbp_df['won'].astype('bool')
        pbp_df.loc[x['team_1_team_id'] == pbp_df['team_id'], 'home'] = x['team_1_is_at_home']
        pbp_df.loc[x['team_2_team_id'] == pbp_df['team_id'], 'home'] = x['team_2_is_at_home']
        pbp_df.loc[x['team_1_team_id'] == pbp_df['team_id'] & ~x['team_1_is_at_home'] & ~x[
            'team_2_is_at_home'], 'home'] = False
        pbp_df.loc[
            x['team_2_team_id'] == pbp_df['team_id'] & ~x['team_1_is_at_home'] & ~x['team_2_is_at_home'], 'home'] = True
        del x
        pbp_df['home'] = pbp_df['home'].astype('bool')
        pbp_df.loc[pbp_df['home'], 'score_diff'] = pbp_df['team_home_score'] - pbp_df['team_visitor_score']
        pbp_df.loc[~pbp_df['home'], 'score_diff'] = pbp_df['team_visitor_score'] - pbp_df['team_home_score']
        pbp_df['score_diff'] = pbp_df['score_diff'].astype('int')

        pbp_df.loc[pbp_df['home'], 'total_score'] = pbp_df['team_home_score'] + pbp_df['team_visitor_score']
        pbp_df.loc[~pbp_df['home'], 'total_score'] = pbp_df['team_visitor_score'] + pbp_df['team_home_score']
        pbp_df['total_score'] = pbp_df['total_score'].astype('int')

        pbp_df['score_diff_calc'] = pbp_df['score_diff'] / np.sqrt(pbp_df['time_remaining'] + 1)
        pbp_df['score_diff_calc'] = pbp_df['score_diff_calc'].astype('float')

        pbp_df.loc[pbp_df['play_type_id'] == 4, 'kickoff'] = True
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

        pbp_df['drive_id'] = np.nan
        pbp_df['drive_sequence'] = np.nan
        pbp_df['points_scored'] = np.nan
        pbp_df.sort_values(by=['game_id','play_sequence'],inplace=True)
        drive_id = 1
        drive_sequence = 1
        game_id = None
        home_score = None
        visitor_score = None
        for index, row in pbp_df.iterrows():
            if game_id is None or row['game_id'] != game_id:
                drive_sequence = 1
                game_id = row['game_id']
                home_score = None
                visitor_score = None
            pbp_df.at[index,'drive_id'] = drive_id
            pbp_df.at[index,'drive_sequence'] = drive_sequence
            #if row['play_type_id'] == 0 and ("End of Quarter" in row['play_summary'] or "Three Minute" in row['play_summary'] or "Game Delayed" in row['play_summary'] or "Timeout" in row['play_summary'] ):
                #pbp_df.at[index,'drive_id'] = np.nan
                #pbp_df.at[index,'drive_sequence'] = np.nan
            if row['play_type_id'] == 4:
                drive_id += 1
                drive_sequence += 1
                #pbp_df.at[index,'drive_id'] = np.nan
                #pbp_df.at[index,'drive_sequence'] = np.nan
            # CFL CONNECT DOESN'T INCLUDE THIS IN DRIVE BUT DOES INCLUDE 2PT attempts? ODD
            #if row['play_type_id'] == 3:
            #    pbp_df.at[index,'drive_id'] = np.nan
            #    pbp_df.at[index,'drive_sequence'] = np.nan
            # TD
            elif row['play_result_type_id'] == 1:
                drive_id += 1
                drive_sequence += 1
            #FG
            elif row['play_result_type_id'] == 2:
                drive_id += 1
                drive_sequence += 1
            #SAFETY
            elif row['play_result_type_id'] == 10:
                drive_id += 1
                drive_sequence += 1
            # CONVERT ATTEMPT
            elif row['play_type_id'] in [3,8,9]:
                drive_id += 1
                drive_sequence += 1
            # END HALF
            elif row['play_result_type_id'] == 8:
                drive_id += 1
                drive_sequence += 1
            # POSSESSION CHANGE (IGNORE CONVERT ATTEMPTS)
            if row['play_change_of_possession_occurred'] and row['play_type_id'] in [1,2,6,7]:
                drive_id += 1
                drive_sequence += 1
            if home_score is None or visitor_score is None:
                home_score = row['team_home_score']
                visitor_score = row['team_visitor_score']
                if home_score !=0 or visitor_score != 0:
                    if row['home']:
                        pbp_df.at[index, 'points_scored'] = home_score - visitor_score
                    else:
                        pbp_df.at[index, 'points_scored'] = visitor_score - home_score
            else:
                if row['home']:
                    pbp_df.at[index, 'points_scored'] = (row['team_home_score']-home_score) - (row['team_visitor_score']-visitor_score)
                else:
                    pbp_df.at[index, 'points_scored'] = (row['team_visitor_score']-visitor_score) -  (row['team_home_score']-home_score)
                home_score = row['team_home_score']
                visitor_score = row['team_visitor_score']

        pbp_df['drive_id'] = pbp_df['drive_id'].astype('Int64')
        pbp_df['drive_sequence'] = pbp_df['drive_sequence'].astype('Int64')
        pbp_df['points_scored'] = pbp_df['points_scored'].astype('Int64')
        #grouped = pbp_df[['drive_sequence','game_id','points_scored']].groupby(by=["game_id",'drive_sequence'], as_index=False).aggregate([np.sum])
        grouped = pbp_df[['game_id','drive_sequence','points_scored']].groupby(by=['game_id','drive_sequence']).apply(sum)[['points_scored']]
        grouped = grouped.reset_index()

        pbp_df = pbp_df.merge(grouped, how='left', left_on=['game_id','drive_sequence'], right_on=['game_id','drive_sequence'])
        pbp_df.rename(columns={"points_scored_x": "points_scored", "points_scored_y": "points_scored_on_drive"}, inplace=True)
        pbp_df['points_scored_on_drive'] = pbp_df['points_scored_on_drive'].astype('Int64')
        model1 = pbp_df[["kickoff", "conv1", "conv2", "regular",
                "distance", "score_diff", "score_diff_calc", "total_score",
                 "time_remaining", "down", "yards_to_go", "ot", "quarter","points_scored_on_drive"]]
        model2 = pbp_df[["kickoff", "conv1", "conv2", "regular",
                "distance", "score_diff", "score_diff_calc", "total_score",
                 "time_remaining", "down", "yards_to_go", "ot", "quarter","won"]]
        model1 = model1.dropna()
        model2 = model2.dropna()
        model1['distance'] = model1['distance'].astype('int')
        model2['distance'] = model2['distance'].astype('int')
        model1['points_scored_on_drive'] = model1['points_scored_on_drive'].astype('int')
        model1 = pd.concat([model1,pd.get_dummies(model1['down'],prefix="down")],axis=1)
        model2 = pd.concat([model2,pd.get_dummies(model2['down'],prefix="down")],axis=1)
        model1 = pd.concat([model1,pd.get_dummies(model1['quarter'],prefix="q")],axis=1)
        model2 = pd.concat([model2,pd.get_dummies(model2['quarter'],prefix="q")],axis=1)
        model1.drop("down", axis=1)
        model1.drop("quarter", axis=1)
        model2.drop("down", axis=1)
        model2.drop("quarter", axis=1)




        def split_model_data(x, y: np.array, seed: int):
            return train_test_split(x, y, stratify=y, test_size=0.3, random_state=seed)

        points_scored_on_drive = np.array(model1.pop('points_scored_on_drive'))
        train, test, train_labels, test_labels = split_model_data(model1, points_scored_on_drive, 12345)
        model_ep = RandomForestRegressor(n_estimators=500,
                                       min_samples_split=2,
                                       max_leaf_nodes=200,
                                       random_state=12345,
                                       max_features='sqrt',
                                       n_jobs=-1, verbose=0)
        model_ep.fit(train, train_labels)

        won = np.array(model2.pop('won'))
        train, test, train_labels, test_labels = split_model_data(model2, won, 12345)
        model_wp = RandomForestClassifier(n_estimators=500,
                                       min_samples_split=2,
                                       max_leaf_nodes=200,
                                       random_state=12345,
                                       max_features='sqrt',
                                       n_jobs=-1, verbose=0)
        model_wp.fit(train, train_labels)

        model_ep.predict(model1)
        model_wp.predict(model2)



        exit()

        pen_df = dataframe_penalties(games_adv)
        rev_df = dataframe_reviews(games_adv)
        ros_df = dataframe_roster(games_adv)
        result_dict_dfs = dataframe_boxscore(games_adv)
        game_ids = games_update_df['game_id'].values
        remove_rows_game_id("games", game_ids)
        remove_rows_game_id("play_by_play", game_ids)
        remove_rows_game_id("penalties", game_ids)
        remove_rows_game_id("roster", game_ids)
        remove_rows_game_id("roster", game_ids)
        for type, value in result_dict_dfs.items():
            for name, df in value.items():
                remove_rows_game_id(f"{type}_{name}", game_ids)



        #store_dataframe(games_df, "games", 'append')
        #store_dataframe(pbp_df, "play_by_play", 'append')
        #store_dataframe(pen_df, "penalties", 'append')
        #store_dataframe(rev_df, "play_reviews", 'append')
        #store_dataframe(ros_df, "roster", 'append')
        #for type, value in result_dict_dfs.items():
        #    for name, df in value.items():
        #        store_dataframe(df, f"{type}_{name}", 'append')

    else:
        print("No games to update!")


# entry
# drive_sequence play
# points_on_drive play
# wp play
# ep play
# epa play
# wpa play


# game_id game
# gei game
# gsi game
# cbf game

main()
