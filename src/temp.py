import datetime
import pickle
import time

import backoff
import numpy as np
from ratelimit import limits, RateLimitException
from backoff import on_exception
from urllib.parse import urljoin

import json
import os
import pathlib
import requests
import sqlite3
import pandas as pd

from handle_game_advanced import dataframe_playbyplay, dataframe_roster, dataframe_reviews, dataframe_penalties
from handle_boxscore import dataframe_boxscore
from handle_game_basic import dataframe_games

MINUTE = 60
API_MAX_CALLS_PER_MINUTE = 30
API_KEY = "vrGdfJ8jdsWHN27QhqtlfVSIFtrhc7NI"
YEAR_START_GAMES = 1958
YEAR_END_GAMES = datetime.datetime.now().date().year
YEAR_CURRENT = datetime.datetime.now().date().year
YEAR_START_ADV = 2004
YEAR_END_ADV = datetime.datetime.now().date().year
URL_CFL = "http://api.cfl.ca/"
URL_V1 = urljoin(URL_CFL, "v1/")
URL_v11 = urljoin(URL_CFL, "v1.1/")
URL_GAMES = urljoin(URL_V1, "games/")
DIR_BASE = pathlib.Path("json")
DIR_GAMES = DIR_BASE.joinpath("games")

history = None


def mkdir(dir_path: os.path) -> None:
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)


def download_games_basic(reset=False) -> None:
    mkdir(DIR_BASE)
    mkdir(DIR_GAMES)
    for year in range(YEAR_START_GAMES, YEAR_END_GAMES + 1, 1):
        download_games_basic_year(year, reset)


def download_games_basic_year(year: int, reset=False) -> None:
    year = str(year)
    url_year = urljoin(URL_GAMES, year)
    dir_basic = DIR_GAMES.joinpath("basic")
    mkdir(dir_basic)
    filename_year = dir_basic.joinpath(f"{year}.json")
    params = {"key": API_KEY}
    if reset and os.path.exists(filename_year):
        os.remove(filename_year)
    if not os.path.exists(filename_year):
        download(url_year, filename_year, params)
    else:
        if check_errors(filename_year):
            os.remove(filename_year)
            download(url_year, filename_year, params)


def backoff_handler(details):
    url = details['args'][0]
    key = details['args'][1]['key']
    include = details['args'][1]['include']
    wait = details['wait']
    tries = details['tries']
    print(f"Backing off:{url}?key={key},include={include} {wait:3.1f}s after {tries}.")


@on_exception(backoff.constant, RateLimitException, max_tries=5, on_backoff=backoff_handler, jitter=None, interval=15)
@limits(calls=API_MAX_CALLS_PER_MINUTE, period=MINUTE)
def download_limited(url: str, params: dict):
    return requests.get(url, params=params)


def download(url: str, filename: os.path, params: dict) -> None:
    global history
    if history is None:
        if os.path.exists("download_history.pkl"):
            with open('download_history.pkl', 'rb') as file:
                history = pickle.load(file)
        else:
            history = []
    full_url = f"{url}?"
    for key, value in params.items():
        full_url += f"{key}={value},"
    print(f"Downloading:{full_url}")
    while True:
        while len(history) >= API_MAX_CALLS_PER_MINUTE:
            while history and (history[-1] - datetime.datetime.now()).seconds > MINUTE:
                history.pop()
            if len(history) >= API_MAX_CALLS_PER_MINUTE:
                time.sleep(1)
        history.insert(0, datetime.datetime.now())
        with open('download_history.pkl', 'wb') as file:
            pickle.dump(history, file)
        response = download_limited(url, params)
        if response.status_code == requests.codes.ok:
            with open(filename, "w") as file:
                if response.text:
                    file.write(response.text)
                    return
        if response.status_code == 429:
            print(f"Too many requests (429) sleep {MINUTE}s and retry!")
            time.sleep(MINUTE)
        else:
            print(f"Other failure code ({response.status_code}) sleep {MINUTE}s and retry!")
            time.sleep(MINUTE)


def check_errors(filename: os.path) -> bool:
    with open(filename) as file:
        text = file.read()
    if not text:
        return True
    json_string = json.loads(text)
    errors = json_string['errors']
    if errors:
        return True
    return False


def load_games_basic(start, end) -> list[dict]:
    games = []
    for year in range(start, end + 1, 1):
        games += load_games_basic_year(year)
    return games


def load_games_basic_year(year: int) -> dict:
    dir_basic = DIR_GAMES.joinpath("basic")
    filename_year = dir_basic.joinpath(f"{year}.json")
    with open(filename_year) as file:
        return json.loads(file.read())['data']


def store_dataframe(df: pd.DataFrame, table_name: str, if_exists) -> None:
    connection = sqlite3.connect("data.db")
    df.to_sql(table_name, connection, if_exists=if_exists, index=False)
    connection.close()


def get_dataframe(table_name: str) -> pd.DataFrame:
    connection = sqlite3.connect("data.db")
    sql_query = pd.read_sql_query(f'''SELECT * FROM {table_name}''', connection)
    connection.close()
    return pd.DataFrame(sql_query)


def download_games_advanced(games_df: pd.DataFrame, start, end, reset=False) -> None:
    mkdir(DIR_BASE)
    mkdir(DIR_GAMES)
    for year in range(start, end + 1, 1):
        download_games_advanced_year(games_df, year, reset)


def download_games_advanced_year(games_df: pd.DataFrame, year: int, reset=False) -> None:
    game_ids = games_df.loc[games_df['year'] == year]['game_id'].values
    for game_id in game_ids:
        download_game_advanced(year, game_id, reset)


def download_game_advanced(year, game_id, reset=False):
    year = str(year)
    dir_advanced = DIR_GAMES.joinpath("advanced")
    mkdir(dir_advanced)
    dir_year = dir_advanced.joinpath(year)
    mkdir(dir_year)
    url_year = urljoin(URL_GAMES, year + "/")
    url_game = urljoin(url_year, f"game/{game_id}/")
    filename_game = dir_year.joinpath(f"{game_id}.json")
    params = {"key": API_KEY, "include": "boxscore,play_by_play,rosters,penalties,play_reviews"}
    if reset and os.path.exists(filename_game):
        os.remove(filename_game)
    if not os.path.exists(filename_game):
        download(url_game, filename_game, params)
    else:
        if check_errors(filename_game):
            os.remove(filename_game)
            download(url_game, filename_game, params)


def load_games_advanced(games_df: pd.DataFrame) -> list[dict]:
    games = []
    for year in range(YEAR_START_ADV, YEAR_END_ADV + 1, 1):
        games += load_games_advanced_year(games_df, year)
    return games


def load_games_advanced_year(games_df: pd.DataFrame, year: int) -> list[dict]:
    games = []
    game_ids = games_df.loc[games_df['year'] == year]['game_id'].values
    for game_id in game_ids:
        games += load_game_advanced(year, game_id)
    return games


def load_game_advanced(year: int, game_id: int) -> dict:
    year = str(year)
    dir_advanced = DIR_GAMES.joinpath("advanced")
    dir_year = dir_advanced.joinpath(year)
    filename_game = dir_year.joinpath(f"{game_id}.json")
    with open(filename_game) as file:
        return json.loads(file.read())['data']


def setup() -> None:
    download_games_basic()
    games = load_games_basic(YEAR_START_GAMES, YEAR_END_GAMES)
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
        f'''SELECT * FROM games WHERE year={YEAR_CURRENT} AND event_status_id=4 AND date_start < CURRENT_DATE AND week=2''',
        connection)
    connection.close()
    return pd.DataFrame(sql_query)


def remove_rows_game_id(table, game_ids):
    connection = sqlite3.connect("data.db")
    connection.execute(f'''DELETE FROM {table} WHERE game_id IN {tuple(game_ids)}''')
    connection.commit()
    connection.close()


def main() -> None:
    # setup()
    games_update_df = games_needing_update()
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

        pbp_df['temp'] = pbp_df['field_position_start'].str[1:]
        pbp_df['temp'].replace([""], np.nan, inplace=True)
        pbp_df['temp'] = pbp_df['temp'].astype('Int64')
        pbp_df.loc[pbp_df['field_position_start'].str[0] == pbp_df['team_abbreviation'].str[0], 'distance'] = 110 - pbp_df['temp']
        pbp_df.loc[pbp_df['field_position_start'].str[0] != pbp_df['team_abbreviation'].str[0], 'distance'] = pbp_df['temp']
        pbp_df['distance'] = pbp_df['distance'].astype('Int64')

        pbp_df.loc[~pbp_df['ot'], 'time_remaining'] = (4-pbp_df['quarter']) * 900 + pbp_df['play_clock_start_in_secs']
        pbp_df.loc[pbp_df['ot'], 'time_remaining'] = 0
        pbp_df['time_remaining'] = pbp_df['time_remaining'].astype('int')

        x = pbp_df.merge(games_df, how='left', left_on='game_id', right_on='game_id')[['game_id', 'play_id', 'team_1_team_id',  'team_1_score','team_1_is_at_home', 'team_1_is_winner', 'team_2_team_id','team_2_abbreviation', 'team_2_score','team_2_is_at_home', 'team_2_is_winner']]
        pbp_df.loc[x['team_1_team_id']==pbp_df['team_id'], 'won'] = x['team_1_is_winner']
        pbp_df.loc[x['team_2_team_id']==pbp_df['team_id'], 'won'] = x['team_2_is_winner']
        pbp_df['won'] = pbp_df['won'].astype('bool')
        pbp_df.loc[x['team_1_team_id']==pbp_df['team_id'], 'home'] = x['team_1_is_at_home']
        pbp_df.loc[x['team_2_team_id']==pbp_df['team_id'], 'home'] = x['team_2_is_at_home']
        pbp_df.loc[x['team_1_team_id']==pbp_df['team_id'] & ~x['team_1_is_at_home'] & ~x['team_2_is_at_home'], 'home'] = False
        pbp_df.loc[x['team_2_team_id']==pbp_df['team_id'] & ~x['team_1_is_at_home'] & ~x['team_2_is_at_home'], 'home'] = True
        pbp_df['home'] = pbp_df['home'].astype('bool')
        pbp_df.loc[pbp_df['home'], 'score_diff'] = pbp_df['team_home_score'] - pbp_df['team_visitor_score']
        pbp_df.loc[~pbp_df['home'], 'score_diff'] = pbp_df['team_visitor_score'] - pbp_df['team_home_score']
        pbp_df['score_diff'] = pbp_df['score_diff'].astype('int')

        pbp_df.loc[pbp_df['home'], 'total_score'] = pbp_df['team_home_score'] + pbp_df['team_visitor_score']
        pbp_df.loc[~pbp_df['home'], 'total_score'] = pbp_df['team_visitor_score'] + pbp_df['team_home_score']
        pbp_df['total_score'] = pbp_df['total_score'].astype('int')

        pbp_df['score_diff_calc'] = pbp_df['score_diff'] / np.sqrt(pbp_df['time_remaining'] + 1)
        pbp_df['score_diff_calc'] = pbp_df['score_diff_calc'].astype('float')

        pbp_df.loc[pbp_df['play_type_id']==4, 'kickoff'] = True
        pbp_df.loc[pbp_df['play_type_id']!=4, 'kickoff'] = False
        pbp_df['kickoff'] = pbp_df['kickoff'].astype('boolean')

        pbp_df.loc[pbp_df['play_type_id']==3, 'conv1'] = True
        pbp_df.loc[pbp_df['play_type_id']!=3, 'conv1'] = False
        pbp_df['conv1'] = pbp_df['conv1'].astype('boolean')

        pbp_df.loc[pbp_df['play_type_id']==8 , 'conv2'] = True
        pbp_df.loc[pbp_df['play_type_id']==9 , 'conv2'] = True
        pbp_df.loc[(pbp_df['play_type_id']!=8) & (pbp_df['play_type_id']!=9), 'conv2'] = False
        pbp_df['conv2'] = pbp_df['conv2'].astype('boolean')

        pbp_df.loc[~pbp_df['kickoff'] & ~pbp_df['conv1'] & ~pbp_df['conv2'], 'regular'] = True
        pbp_df.loc[pbp_df['kickoff'] | pbp_df['conv1'] | pbp_df['conv2'], 'regular'] = False
        pbp_df['regular'] = pbp_df['regular'].astype('boolean')

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

        store_dataframe(games_df, "games", 'append')
        store_dataframe(pbp_df, "play_by_play", 'append')
        store_dataframe(pen_df, "penalties", 'append')
        store_dataframe(rev_df, "play_reviews", 'append')
        store_dataframe(ros_df, "roster", 'append')
        for type, value in result_dict_dfs.items():
            for name, df in value.items():
                store_dataframe(df, f"{type}_{name}", 'append')

    else:
        print("No games to update!")

# entry
# drive_sequence play
# regular play
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
