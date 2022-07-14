from typing import Union

import pandas as pd
from discreteMarkovChain import markovChain
import numpy as np

from store import store_helper

NAMES_8 = ["BC", "CGY", "EDM", "HAM", "MTL", "SSK", "TOR", "WPG"]
NAMES_9 = ["BC", "CGY", "EDM", "HAM", "MTL", "OTT", "SSK", "TOR", "WPG"]


def main() -> None:
    print("2022 Regular Season SCORE Rankings")
    store = model_wp(2022, regular_season_only=False, limit=None)
    for team, score in sorted(store.items(), key=lambda x: x[1], reverse=True):
        print(f"{team:4s}\t{score:.4f}")
    print(sum(store.values()))


def model_wp(year: int, regular_season_only: bool = True, limit: Union[int, None] = None) -> dict[str, dict]:
    names = NAMES_9
    if 2006 <= year <= 2013:
        names = NAMES_8
    size = len(names)
    data = get_wp(year, regular_season_only, limit)
    sums = np.zeros((size, size))
    count = np.zeros((size, size))
    for i, row in data.iterrows():
        team1 = int(row['team_1_team_id'])
        team2 = int(row['team_2_team_id'])
        if 2006 <= year <= 2013 and team1 >= 6:
            team1 -= 1
        if 2006 <= year <= 2013 and team2 >= 6:
            team2 -= 1
        sums[team1 - 1][team2 - 1] += row['team_2_wp']
        sums[team2 - 1][team1 - 1] += row['team_1_wp']
        count[team1 - 1][team2 - 1] += 1
        count[team2 - 1][team1 - 1] += 1
    for i in range(len(names)):
        for j in range(len(names)):
            if count[i][j] != 0:
                sums[i][j] = sums[i][j] / count[i][j]
    mc = markovChain(sums)
    mc.computePi('linear')  # We can also use 'power', 'krylov' or 'eigen'
    zipped = zip(names, mc.pi * len(names) / 2)
    store = {}
    for team, value in zipped:
        store[team] = value
    return store


def get_wp(year: int, regular_season_only: bool = True, limit: Union[int, None] = None) -> pd.DataFrame:
    if regular_season_only:
        regular = "AND event_type_id = 1"
    else:
        regular = "AND event_type_id >= 1"
    query = f'''SELECT team_1_team_id, team_2_team_id, team_1_wp, team_2_wp, games.game_id FROM epa LEFT JOIN games ON games.game_id = epa.game_id WHERE games.year = {year} {regular} AND event_status_id=4 ORDER BY games.date_start'''
    dataframe = store_helper.load_dataframe_from_query(query)
    dataframe.dropna(inplace=True)
    games = dataframe.drop_duplicates(subset=["game_id"])["game_id"]
    if limit:
        games.head(limit)
    games = games.values.tolist()
    dataframe = dataframe[dataframe['game_id'].isin(games)]
    dataframe = dataframe.groupby(["game_id"]).aggregate(
        {'team_1_team_id': ['min'], 'team_2_team_id': ['min'], 'team_1_wp': ['mean'],
         'team_2_wp': ['mean']}).reset_index()
    dataframe = dataframe.droplevel(level=1, axis=1)
    dataframe['team_1_team_id'] = dataframe['team_1_team_id'].astype(int)
    dataframe['team_2_team_id'] = dataframe['team_2_team_id'].astype(int)
    return dataframe[["team_1_team_id", "team_2_team_id", "team_1_wp", "team_2_wp"]]


if __name__ == "__main__":
    main()
