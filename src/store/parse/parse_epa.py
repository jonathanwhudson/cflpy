import os
import pickle

import numpy as np
import pandas as pd

import config


def parse_epa(epa_df: pd.DataFrame) -> pd.DataFrame:
    train_df = epa_df[['play_id', "kickoff", "conv1", "conv2", "regular",
                       "distance", "score_diff", "score_diff_calc", "total_score",
                       "time_remaining", "down", "yards_to_go", "ot", "quarter"]]
    train_df = train_df.dropna()
    train_df['down'] = train_df['down'].astype("float")
    train_df = pd.concat([train_df, pd.get_dummies(train_df['down'], prefix="down")], axis=1)
    train_df = pd.concat([train_df, pd.get_dummies(train_df['quarter'], prefix="q")], axis=1)
    train_df.drop("down", axis=1, inplace=True)
    train_df.drop("quarter", axis=1, inplace=True)
    if 'q_5' not in train_df.columns:
        train_df['q_5'] = 0
    train_df['q_5'] = train_df['q_5'].astype("uint8")
    play_id = np.array(train_df.pop('play_id'))
    if os.path.exists(config.FILE_MODEL_EP):
        model_ep = pickle.load(open(config.FILE_MODEL_EP, "rb"))
    else:
        raise Exception("EP model does not exist to load!")
    if os.path.exists(config.FILE_MODEL_WP):
        model_wp = pickle.load(open(config.FILE_MODEL_WP, "rb"))
    else:
        raise Exception("WP model does not exist to load!")

    predict_ep = model_ep.predict(train_df)
    predict_wp = model_wp.predict_proba(train_df)[:, 1]
    del model_ep, model_wp, train_df

    results = pd.DataFrame(np.vstack([play_id, predict_ep, predict_wp]).T, columns=['play_id', 'ep', 'wp'])

    epa_df = epa_df.merge(results, how='left', left_on=['play_id'], right_on=['play_id'])
    del play_id, predict_ep, predict_wp, results

    epa_df['home'] = epa_df['home'].astype(bool)
    epa_df['last_play'] = epa_df['last_play'].astype(bool)
    epa_df.loc[epa_df['home'], "team_1_wp"] = 1 - epa_df['wp']
    epa_df.loc[~epa_df['home'], "team_1_wp"] = epa_df['wp']
    epa_df.loc[epa_df['home'], "team_2_wp"] = epa_df['wp']
    epa_df.loc[~epa_df['home'], "team_2_wp"] = 1 - epa_df['wp']

    game_id = None
    for index, row in epa_df.iterrows():
        if game_id is None or game_id != row['game_id']:
            epa_df.at[index, 'team_1_wpa'] = row['team_1_wp'] - 0.5
            epa_df.at[index, 'team_2_wpa'] = row['team_2_wp'] - 0.5
        elif row['play_result_type_id'] == 9 or index == len(epa_df) - 1:
            epa_df.at[index, 'team_1_wpa'] = row['team_1_wp'] - row['team_1_is_winner']
            epa_df.at[index, 'team_2_wpa'] = row['team_2_wp'] - row['team_2_is_winner']
        elif row['entry'] == 2:
            epa_df.at[index, 'team_1_wpa'] = epa_df.at[index + 2, 'team_1_wp'] - epa_df.at[index, 'team_1_wp']
            epa_df.at[index, 'team_2_wpa'] = epa_df.at[index + 2, 'team_2_wp'] - epa_df.at[index, 'team_2_wp']
        else:
            epa_df.at[index, 'team_1_wpa'] = epa_df.at[index + 1, 'team_1_wp'] - epa_df.at[index, 'team_1_wp']
            epa_df.at[index, 'team_2_wpa'] = epa_df.at[index + 1, 'team_2_wp'] - epa_df.at[index, 'team_2_wp']
        game_id = row['game_id']

    epa_df.loc[epa_df['home'], "wpa"] = epa_df['team_2_wpa']
    epa_df.loc[~epa_df['home'], "wpa"] = epa_df['team_1_wpa']
    del game_id, index, row

    epa_df.loc[epa_df['home'], "team_1_ep"] = -epa_df['ep']
    epa_df.loc[~epa_df['home'], "team_1_ep"] = epa_df['ep']
    epa_df.loc[epa_df['home'], "team_2_ep"] = epa_df['ep']
    epa_df.loc[~epa_df['home'], "team_2_ep"] = -epa_df['ep']

    game_id = None
    for index, row in epa_df.iterrows():
        if game_id is None or game_id != row['game_id']:
            epa_df.at[index, 'team_1_epa'] = row['team_1_ep']
            epa_df.at[index, 'team_2_epa'] = row['team_2_ep']
        elif row['last_play'] or index == len(epa_df) - 1:
            epa_df.at[index, 'team_1_epa'] = row['points_scored_on_drive'] - row['team_1_ep']
            epa_df.at[index, 'team_2_epa'] = row['points_scored_on_drive'] - row['team_2_ep']
        elif row['entry'] == 2:
            epa_df.at[index, 'team_1_epa'] = epa_df.at[index + 2, 'team_1_ep'] - epa_df.at[index, 'team_1_ep']
            epa_df.at[index, 'team_2_epa'] = epa_df.at[index + 2, 'team_2_ep'] - epa_df.at[index, 'team_2_ep']
        else:
            epa_df.at[index, 'team_1_epa'] = epa_df.at[index + 1, 'team_1_ep'] - epa_df.at[index, 'team_1_ep']
            epa_df.at[index, 'team_2_epa'] = epa_df.at[index + 1, 'team_2_ep'] - epa_df.at[index, 'team_2_ep']
        game_id = row['game_id']

    epa_df.loc[epa_df['home'], "epa"] = epa_df['team_2_epa']
    epa_df.loc[~epa_df['home'], "epa"] = epa_df['team_1_epa']
    del game_id, index, row

    epa_df = epa_df[
        ['year', 'game_id', 'play_id', 'play_sequence', 'entry',
         "ep", "epa", "team_1_ep", "team_2_ep", "team_1_epa", "team_2_epa",
         "wp", "wpa", "team_1_wp", "team_2_wp", "team_1_wpa", "team_2_wpa"]]
    return epa_df
