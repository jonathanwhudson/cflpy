import os
import pickle

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.model_selection import train_test_split

import config
import store

epa_df = store.query(f"""SELECT drives.*, pbp.play_result_type_id,games.team_1_is_winner,games.team_2_is_winner FROM drives LEFT JOIN pbp ON pbp.play_id=drives.play_id and drives.entry=pbp.entry LEFT JOIN games ON games.game_id=drives.game_id WHERE drives.year >= {config.YEAR_START_ADV_USEFUL}""")

train_df = epa_df[['play_id', "kickoff", "conv1", "conv2", "regular",
                   "distance", "score_diff", "score_diff_calc", "total_score",
                   "time_remaining", "down", "yards_to_go", "ot", "quarter", "points_scored_on_drive", "won"]]
train_df = train_df.dropna()
train_df = pd.concat([train_df, pd.get_dummies(train_df['down'], prefix="down")], axis=1)
train_df = pd.concat([train_df, pd.get_dummies(train_df['quarter'], prefix="q")], axis=1)
train_df.drop("down", axis=1, inplace=True)
train_df.drop("quarter", axis=1, inplace=True)


def split_model_data(x, y: np.array, seed: int):
    return train_test_split(x, y, stratify=y, test_size=0.3, random_state=seed)


play_id = np.array(train_df.pop('play_id'))
points_scored_on_drive = np.array(train_df.pop('points_scored_on_drive'))
won = np.array(train_df.pop('won'))
if os.path.exists("data/ep.mdl"):
    print("Loading ep")
    model_ep = pickle.load(open("data/ep.mdl","rb"))
else:
    train, test, train_labels, test_labels = split_model_data(train_df, points_scored_on_drive, 12345)
    model_ep = RandomForestRegressor(n_estimators=500,
                                     min_samples_split=2,
                                     max_leaf_nodes=200,
                                     random_state=12345,
                                     max_features='sqrt',
                                     n_jobs=-1, verbose=0)
    model_ep.fit(train, train_labels)
    print("Saving ep")
    pickle.dump(model_ep, open("data/ep.mdl", "wb"))
    del train, test, train_labels, test_labels

if os.path.exists("data/wp.mdl"):
    print("Loading wp")
    model_wp = pickle.load(open("data/wp.mdl","rb"))
else:
    train, test, train_labels, test_labels = split_model_data(train_df, won, 12345)
    model_wp = RandomForestClassifier(n_estimators=500,
                                      min_samples_split=2,
                                      max_leaf_nodes=200,
                                      random_state=12345,
                                      max_features='sqrt',
                                      n_jobs=-1, verbose=0)
    model_wp.fit(train, train_labels)
    print("Saving wp")
    pickle.dump(model_wp, open("data/wp.mdl", "wb"))
    del train, test, train_labels, test_labels

print("Predicting EP")
predict_ep = model_ep.predict(train_df)
print("Predicting WP")
predict_wp = model_wp.predict_proba(train_df)[:, 1]
print("Done predicting")
del points_scored_on_drive, won, model_ep, model_wp, train_df

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
    elif row['play_result_type_id'] == 9:
        epa_df.at[index, 'team_1_wpa'] = row['team_1_wp'] - row['team_1_is_winner']
        epa_df.at[index, 'team_2_wpa'] = row['team_2_wp'] - row['team_2_is_winner']
    elif row['entry'] == 2:
        epa_df.at[index, 'team_1_wpa'] = epa_df.at[index+2, 'team_1_wp']-epa_df.at[index, 'team_1_wp']
        epa_df.at[index, 'team_2_wpa'] = epa_df.at[index+2, 'team_2_wp']-epa_df.at[index, 'team_2_wp']
    else:
        epa_df.at[index, 'team_1_wpa'] = epa_df.at[index+1, 'team_1_wp']-epa_df.at[index, 'team_1_wp']
        epa_df.at[index, 'team_2_wpa'] = epa_df.at[index+1, 'team_2_wp']-epa_df.at[index, 'team_2_wp']
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
    elif row['last_play']:
        epa_df.at[index, 'team_1_epa'] = row['points_scored_on_drive'] - row['team_1_ep']
        epa_df.at[index, 'team_2_epa'] = row['points_scored_on_drive'] - row['team_2_ep']
    elif row['entry'] == 2:
        epa_df.at[index, 'team_1_epa'] = epa_df.at[index+2, 'team_1_ep']-epa_df.at[index, 'team_1_ep']
        epa_df.at[index, 'team_2_epa'] = epa_df.at[index+2, 'team_2_ep']-epa_df.at[index, 'team_2_ep']
    else:
        epa_df.at[index, 'team_1_epa'] = epa_df.at[index+1, 'team_1_ep']-epa_df.at[index, 'team_1_ep']
        epa_df.at[index, 'team_2_epa'] = epa_df.at[index+1, 'team_2_ep']-epa_df.at[index, 'team_2_ep']
    game_id = row['game_id']

epa_df.loc[epa_df['home'], "epa"] = epa_df['team_2_epa']
epa_df.loc[~epa_df['home'], "epa"] = epa_df['team_1_epa']
del game_id, index, row

epa_df = epa_df[
    ['year', 'game_id', 'play_id','play_sequence','entry',
     "ep","epa","team_1_ep","team_2_ep","team_1_epa","team_2_epa",
     "wp","wpa","team_1_wp","team_2_wp","team_1_wpa","team_2_wpa"]]

store.store_dataframe(epa_df, "epa", if_exists=store.IF_EXISTS_REPLACE)
print("Done")
