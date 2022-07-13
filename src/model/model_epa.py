import pickle

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier

import config
from store import store_helper


def train_models(year_start: int, year_end: int) -> None:
    query = f"""SELECT drives.*, pbp.play_result_type_id,games.team_1_is_winner,games.team_2_is_winner FROM drives LEFT JOIN pbp ON pbp.play_id=drives.play_id and drives.entry=pbp.entry LEFT JOIN games ON games.game_id=drives.game_id WHERE drives.year >= {year_start} AND drives.year <= {year_end}"""
    epa_df = store_helper.load_dataframe_from_query(query)

    train_df = epa_df[["kickoff", "conv1", "conv2", "regular",
                       "distance", "score_diff", "score_diff_calc", "total_score",
                       "time_remaining", "down", "yards_to_go", "ot", "quarter", "points_scored_on_drive", "won"]]
    train_df = train_df.dropna()
    train_df = pd.concat([train_df, pd.get_dummies(train_df['down'], prefix="down")], axis=1)
    train_df = pd.concat([train_df, pd.get_dummies(train_df['quarter'], prefix="q")], axis=1)
    train_df.drop("down", axis=1, inplace=True)
    train_df.drop("quarter", axis=1, inplace=True)

    points_scored_on_drive = np.array(train_df.pop('points_scored_on_drive'))
    won = np.array(train_df.pop('won'))
    # train, test, train_labels, test_labels = split_model_data(train_df, points_scored_on_drive, 12345)
    model_ep = RandomForestRegressor(n_estimators=500,
                                     min_samples_split=2,
                                     max_leaf_nodes=200,
                                     random_state=12345,
                                     max_features='sqrt',
                                     n_jobs=-1, verbose=0)
    model_ep.fit(train_df, points_scored_on_drive)
    # model_ep.fit(train, train_labels)
    # print(f"EP Model accuracy: {model_ep.score(test, test_labels):.2f} R^2")
    print("Saving EP")
    pickle.dump(model_ep, open(config.FILE_MODEL_EP, "wb"))
    # del train, test, train_labels, test_labels
    # train, test, train_labels, test_labels = split_model_data(train_df, won, 12345)
    model_wp = RandomForestClassifier(n_estimators=500,
                                      min_samples_split=2,
                                      max_leaf_nodes=200,
                                      random_state=12345,
                                      max_features='sqrt',
                                      n_jobs=-1, verbose=0)
    model_wp.fit(train_df, won)
    # model_wp.fit(train, train_labels)
    # print(f"WP Model accuracy: {model_wp.score(test, test_labels) * 100:.0f}%")
    print("Saving WP")
    pickle.dump(model_wp, open(config.FILE_MODEL_WP, "wb"))
    # del train, test, train_labels, test_labels


# def split_model_data(x, y: np.array, seed: int):
#     return train_test_split(x, y, stratify=y, test_size=0.3, random_state=seed)

def main() -> None:
    train_models(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
