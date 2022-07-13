import os
import pickle
import sys
import time

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split

from cfldb.cfldb import cfldb, transform_table_names
from cfldb.config import config

columns_id = ["play_id", "entry"]
columns_model = ["kickoff", "conv1", "conv2", "regular",
                 "dist_from_end_zone", "score_diff", "score_diff_calc", "total_score",
                 "time_remaining", "down", "yards_to_go", "ot"]
columns_wp = ["won"]
columns_ep = ["points_on_drive"]


def main() -> None:
    model_wp, model_ep = get_models()
    kickoff = False
    conv1 = False
    conv2 = False
    regular = True
    dist_from_end_zone = 100
    score_diff = -3
    total_score = 50
    down = 1
    yard_to_go = 10
    ot = False
    time_remaining = 15
    for i in range(-50,51,1):
        score_diff = i
        score_diff_calc = score_diff / (time_remaining + 1) ** (0.5)
        query(model_wp, model_ep,kickoff, conv1, conv2, regular, dist_from_end_zone, score_diff, score_diff_calc, total_score, i,down,yard_to_go,ot)



def get_models() -> (RandomForestClassifier, RandomForestRegressor):
    model_wp = get_model_wp(config.model_name_wp)
    model_ep = get_model_ep(config.model_name_ep,)
    return model_wp, model_ep


def get_model_wp(model_name: str) -> RandomForestClassifier:
    print("Load WP model.")
    model = pickle.load(open(model_name, 'rb'))
    return model


def get_model_ep(model_name: str) -> RandomForestRegressor:
    print("Load EP model.")
    model = pickle.load(open(model_name, 'rb'))
    return model


def split_model_data(x, y: np.array, seed: int):
    return train_test_split(x, y, stratify=y, test_size=0.3, random_state=seed)


def query(model_wp: RandomForestClassifier, model_ep: RandomForestRegressor,kickoff, conv1, conv2, regular, dist_from_end_zone, score_diff, score_diff_calc, total_score, time_remaining,down,yard_to_go,ot):
    info = [kickoff, conv1, conv2, regular, dist_from_end_zone, score_diff, score_diff_calc, total_score, time_remaining,down,yard_to_go,ot]
    win_prob_predict = model_wp.predict_proba([info])
    exp_pts_predict = model_ep.predict([info])
    print(f"{win_prob_predict[0][1]*100:.2f}%, {exp_pts_predict[0]:.2f}")


main()
