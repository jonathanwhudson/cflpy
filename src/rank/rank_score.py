
from typing import Union

from discreteMarkovChain import markovChain
import numpy as np

from store import store_helper


def main() -> None:
    print()
    print("2022 Regular Season SCORE Rankings")
    ranking(2022, 2022, regular_season_only=False, limit=None)
    # print()
    # print("Regular Season SCORE Rankings (37 games)")
    # ranking(2009, 2022, regular_season_only=True, limit=39)
    # print()
    # print("Regular Season SCORE Rankings (full)")
    # ranking(2009, 2022, regular_season_only=True, limit=None)
    # print()
    # print("Regular Season SCORE Rankings (full & playoffs)")
    # ranking(2009, 2022, regular_season_only=False, limit=None)
    # print()


def ranking(start: int, end: int, regular_season_only: bool = True, limit: Union[int, None] = None) -> dict[str,dict]:
    names1 = ["BC", "CGY", "EDM", "HAM", "MTL", "OTT", "SSK", "TOR", "WPG"]
    names2 = ["BC", "CGY", "EDM", "HAM", "MTL", "SSK", "TOR", "WPG"]
    store = {}
    for name in names1:
        store[name] = {}
        for year in range(start, end + 1):
            store[name][year] = 0
    for year in range(start, end + 1):
        if year == 2020:
            continue
        x, t1 = get_info(year, regular_season_only, limit)
        teams = 9
        if 2006 <= year <= 2013:
            teams = 8
        sums = np.zeros((teams, teams))
        count = np.zeros((teams, teams))
        for i in range(len(x)):
            team1 = t1[i][0]
            team2 = t1[i][1]
            if 2006 <= year <= 2013 and team1 >= 6:
                team1 -= 1
            if 2006 <= year <= 2013 and team2 >= 6:
                team2 -= 1
            sums[team1 - 1][team2 - 1] += x[i][1]
            sums[team2 - 1][team1 - 1] += x[i][0]
            count[team1 - 1][team2 - 1] += 1
            count[team2 - 1][team1 - 1] += 1
        for i in range(teams):
            for j in range(teams):
                if count[i][j] != 0:
                    sums[i][j] = sums[i][j] / count[i][j]
        mc = markovChain(sums)
        mc.computePi('linear')  # We can also use 'power', 'krylov' or 'eigen'
        if 2006 <= year <= 2013:
            zipped = zip(names2, mc.pi * 4)
        else:
            zipped = zip(names1, mc.pi * 4.5)
        zipped = list(zipped)
        zipped = sorted(zipped, key=lambda entry: entry[1], reverse=True)

        if start == end:
            print("%s\t%6s" % ("TEAM", "SCORE"))
        for x, y in zipped:
            if start == end:
                print(f"{x:4s}\t{y:.4f}")
            store[x][year] = y
    if start != end:
        print("%3s" % "", end="\t")
        for year in range(start, end + 1):
            if year == 2020:
                continue
            print("%6d" % year, end="\t")
        print()
        for name in names1:
            print("%3s" % name, end="\t")
            for year in range(start, end + 1):
                if year == 2020:
                    continue
                print("%6.4f" % store[name][year], end="\t")
            print()

def model(year: int, regular_season_only: bool = True, limit: Union[int, None] = None) -> dict[str,dict]:
    names1 = ["BC", "CGY", "EDM", "HAM", "MTL", "OTT", "SSK", "TOR", "WPG"]
    names2 = ["BC", "CGY", "EDM", "HAM", "MTL", "SSK", "TOR", "WPG"]
    store = {}
    for name in names1:
        store[name] = {}
        store[name] = 0
    x, t1 = get_info(year, regular_season_only, limit)
    teams = 9
    if 2006 <= year <= 2013:
        teams = 8
    sums = np.zeros((teams, teams))
    count = np.zeros((teams, teams))
    for i in range(len(x)):
        team1 = t1[i][0]
        team2 = t1[i][1]
        if 2006 <= year <= 2013 and team1 >= 6:
            team1 -= 1
        if 2006 <= year <= 2013 and team2 >= 6:
            team2 -= 1
        sums[team1 - 1][team2 - 1] += x[i][1]
        sums[team2 - 1][team1 - 1] += x[i][0]
        count[team1 - 1][team2 - 1] += 1
        count[team2 - 1][team1 - 1] += 1
    for i in range(teams):
        for j in range(teams):
            if count[i][j] != 0:
                sums[i][j] = sums[i][j] / count[i][j]
    mc = markovChain(sums)
    mc.computePi('linear')  # We can also use 'power', 'krylov' or 'eigen'
    if 2006 <= year <= 2013:
        zipped = zip(names2, mc.pi * 4)
    else:
        zipped = zip(names1, mc.pi * 4.5)
    zipped = list(zipped)
    zipped = sorted(zipped, key=lambda entry: entry[1], reverse=True)

    for x, y in zipped:
        store[x] = y
    return store


def get_info(year, regular_season_only=True, limit=None) -> (list[(int, int)], list[(int, int)]):
    regular = ""
    if regular_season_only:
        regular = "AND event_type_id = 1"

    query = f'''
        SELECT team_1_team_id, team_2_team_id, team_1_score, team_2_score
        FROM games
        WHERE year = {year}
        {regular}
        ORDER BY games.game_id
    '''
    results1 = store_helper.load_dataframe_from_query(query)
    te = []
    y = []
    for index, row in results1.iterrows():
        te.append((row['team_1_team_id'], row['team_2_team_id']))
        y.append((row['team_1_score'], row['team_2_score']))
        if limit is not None and len(te) == limit:
            break
    return y, te


if __name__ == "__main__":
    main()
