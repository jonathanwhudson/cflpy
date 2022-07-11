import os
import pickle
import sqlite3

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.model_selection import train_test_split

import column_dtypes
import config
import helper
import store


def load_epa_year_range(start: int, end: int, limit_years: list[int] = None) -> pd.DataFrame:
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not loading any EPA as nothing in years=[{start},{end}]!")
    if limit_years:
        limit_years = set(limit_years)
        years = years.intersection(limit_years)
        if not years:
            raise ValueError(
                f"Not loading any EPA as nothing in years=[{start},{end}] due to limit_years={limit_years}!")
    epa_df = load_epa(list(years), game_ids=None)
    return helper.clean_and_order_columns(epa_df, column_dtypes.EPA_COLUMNS)


def load_epa_year(year: int) -> pd.DataFrame:
    if year == 2020:
        raise ValueError(f"Not loading EPA as year={year} did not get played!")
    epa_df = load_epa([year], game_ids=None)
    return helper.clean_and_order_columns(epa_df, column_dtypes.EPA_COLUMNS)


def load_epa_games(game_ids: list[int]) -> pd.DataFrame:
    epa_df = load_epa(years=None, game_ids=game_ids)
    return helper.clean_and_order_columns(epa_df, column_dtypes.EPA_COLUMNS)


def load_epa_game(game_id: int) -> pd.DataFrame:
    epa_df = load_epa(years=None, game_ids=[game_id])
    return helper.clean_and_order_columns(epa_df, column_dtypes.EPA_COLUMNS)


def remove_epa_year_range(start: int, end: int, limit_years: list[int] = None) -> None:
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not removing any EPA as nothing in years=[{start},{end}]!")
    if limit_years:
        limit_years = set(limit_years)
        years = limit_years.intersection(years)
        if years:
            raise ValueError(
                f"Not removing any EPA as nothing in years=[{start},{end}] due to limit_years={limit_years}!")
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(
        f"""SELECT DISTINCT(game_id) FROM epa WHERE year in {str(tuple(years)).replace(",)", ")")}""",
        connection)
    connection.close()
    game_ids = dataframe['game_id'].values.tolist()
    remove_epa_games(game_ids)


def remove_epa_year(year: int) -> None:
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(f"SELECT DISTINCT(game_id) FROM epa WHERE year={year}", connection)
    game_ids = dataframe['game_id'].values.tolist()
    connection.close()
    remove_epa_games(game_ids)


def remove_epa_games(game_ids: list[int]):
    game_ids = set(game_ids)
    connection = sqlite3.connect(config.DB_FILE)
    connection.execute(f'''DELETE FROM epa WHERE game_id in {str(tuple(game_ids)).replace(",)", ")")}''')
    connection.commit()
    connection.close()


def remove_epa_game(game_id: int):
    remove_epa_games([game_id])


def split_model_data(x, y: np.array, seed: int):
    return train_test_split(x, y, stratify=y, test_size=0.3, random_state=seed)


def train_models(year_start: int, year_end: int) -> None:
    epa_df = store.query(
        f"""SELECT drives.*, pbp.play_result_type_id,games.team_1_is_winner,games.team_2_is_winner FROM drives LEFT JOIN pbp ON pbp.play_id=drives.play_id and drives.entry=pbp.entry LEFT JOIN games ON games.game_id=drives.game_id WHERE drives.year >= {year_start} AND drives.year <= {year_end}""")

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
    train, test, train_labels, test_labels = split_model_data(train_df, points_scored_on_drive, 12345)
    model_ep = RandomForestRegressor(n_estimators=500,
                                     min_samples_split=2,
                                     max_leaf_nodes=200,
                                     random_state=12345,
                                     max_features='sqrt',
                                     n_jobs=-1, verbose=0)
    model_ep.fit(train, train_labels)
    print(f"EP Model accuracy: {model_ep.score(test, test_labels):.2f} R^2")
    print("Saving EP")
    pickle.dump(model_ep, open(config.MD_EP_FILE, "wb"))
    del train, test, train_labels, test_labels
    train, test, train_labels, test_labels = split_model_data(train_df, won, 12345)
    model_wp = RandomForestClassifier(n_estimators=500,
                                      min_samples_split=2,
                                      max_leaf_nodes=200,
                                      random_state=12345,
                                      max_features='sqrt',
                                      n_jobs=-1, verbose=0)
    model_wp.fit(train, train_labels)
    print(f"WP Model accuracy: {model_wp.score(test, test_labels) * 100:.0f}%")
    print("Saving WP")
    pickle.dump(model_wp, open(config.MD_WP_FILE, "wb"))
    del train, test, train_labels, test_labels


def load_epa(years: list[int] = None, game_ids: list[int] = None) -> pd.DataFrame:
    if not years and not game_ids:
        raise ValueError(f"Not loading any EPA as no year or game_ids were given!")
    add = ""
    if years:
        years = set(years)
        add += f"games.year in {str(tuple(years)).replace(',)', ')')}"
    if years and game_ids:
        add += " AND "
    if game_ids:
        game_ids = set(game_ids)
        add += f"""games.game_id in {str(tuple(game_ids)).replace(",)", ")")}"""
    query = f"""SELECT drives.*, pbp.play_summary, pbp.play_result_type_id,games.team_1_is_winner,games.team_2_is_winner FROM drives LEFT JOIN pbp ON pbp.play_id=drives.play_id and drives.entry=pbp.entry LEFT JOIN games ON games.game_id=drives.game_id WHERE {add}"""
    epa_df = store.query(query)

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
    if os.path.exists(config.MD_EP_FILE):
        model_ep = pickle.load(open(config.MD_EP_FILE, "rb"))
    else:
        raise Exception("EP model does not exist to load!")
    if os.path.exists(config.MD_WP_FILE):
        model_wp = pickle.load(open(config.MD_WP_FILE, "rb"))
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


def store_epa(epa_df: pd.DataFrame, if_exists: str = store.IF_EXISTS_REPLACE):
    store.store_dataframe(epa_df, "epa", if_exists=if_exists)


def reset_epa_all() -> None:
    drives_df = load_epa_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    remove_epa_year_range(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    store_epa(drives_df, store.IF_EXISTS_REPLACE)


def reset_epa_year(year: int) -> None:
    drives_df = load_epa_year(year)
    remove_epa_year(year)
    store_epa(drives_df, store.IF_EXISTS_APPEND)


def reset_epa_current_year() -> None:
    reset_epa_year(config.YEAR_CURRENT)


def reset_epa_games(game_ids: list[int]) -> None:
    drives_df = load_epa_games(game_ids)
    remove_epa_games(game_ids)
    store_epa(drives_df, store.IF_EXISTS_APPEND)


def reset_epa_game(game_id: int) -> None:
    drives_df = load_epa_game(game_id)
    remove_epa_game(game_id)
    store_epa(drives_df, store.IF_EXISTS_APPEND)


def main() -> None:
    # Reset all drives (note that this should be followed by GEI and others that require EPA info)
    if False:
        print("Train models")
        train_models(config.YEAR_START_ADV_USEFUL, config.YEAR_END_ADV)
    # Reset all drives (note that this should be followed by GEI and others that require EPA info)
    if False:
        print("Resetting all EPA")
        reset_epa_all()
    # Reset all drives for current year (note that this should be followed by GEI and others that require EPA info)
    if True:
        print("Resetting EPA for current year")
        reset_epa_current_year()
    # Reset all drives for chosen year (note that this should be followed by GEI and others that require EPA info)
    if False:
        year = 2022
        print(f"Resetting EPA for year=<{year}>")
        reset_epa_year(year)
    # Reset all drives for certain game(note that this should be followed by GEI and others that require EPA info)
    if False:
        game_ids = [6227, 6228, 6229]
        print(f"Resetting EPA for games=<{game_ids}>")
        reset_epa_games(game_ids)
    # Reset all drives for certain game(note that this should be followed by GEI and others that require EPA info)
    if False:
        game_id = 6229
        print(f"Resetting EPA for game=<{game_id}>")
        reset_epa_game(game_id)


if __name__ == '__main__':
    main()
    print("Done")
