from typing import Union

import pandas as pd
from discreteMarkovChain import markovChain
import numpy as np

from store import store_helper

NAMES_8 = ["BC", "CGY", "EDM", "HAM", "MTL", "SSK", "TOR", "WPG"]
NAMES_9 = ["BC", "CGY", "EDM", "HAM", "MTL", "OTT", "SSK", "TOR", "WPG"]


def main() -> None:
    print("2022 Regular Season SCORE Rankings")
    store = model_score(2022, regular_season_only=False, limit=None)
    for team, score in sorted(store.items(), key=lambda x: x[1], reverse=True):
        print(f"{team:4s}\t{score:.4f}")


def model_score(year: int, regular_season_only: bool = True, limit: Union[int, None] = None) -> dict[str, dict]:
    names = NAMES_9
    if 2006 <= year <= 2013:
        names = NAMES_8
    size = len(names)
    data = get_scores(year, regular_season_only, limit)
    sums = np.zeros((size, size))
    count = np.zeros((size, size))
    for i, row in data.iterrows():
        team1 = row['team_1_team_id']
        team2 = row['team_2_team_id']
        if 2006 <= year <= 2013 and team1 >= 6:
            team1 -= 1
        if 2006 <= year <= 2013 and team2 >= 6:
            team2 -= 1
        sums[team1 - 1][team2 - 1] += row['team_2_score']
        sums[team2 - 1][team1 - 1] += row['team_1_score']
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


def get_scores(year: int, regular_season_only: bool = True, limit: Union[int, None] = None) -> pd.DataFrame:
    if regular_season_only:
        regular = "AND event_type_id = 1"
    else:
        regular = "AND event_type_id >= 1"
    query = f'''SELECT team_1_team_id, team_2_team_id, team_1_score, team_2_score FROM games WHERE year = {year} {regular} AND event_status_id=4 ORDER BY games.date_start'''
    dataframe = store_helper.load_dataframe_from_query(query)
    if limit:
        dataframe.head(limit)
    return dataframe


if __name__ == "__main__":
    main()
