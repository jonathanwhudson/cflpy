from typing import Union

from discreteMarkovChain import markovChain
import numpy as np

from store import store_helper


def main() -> None:
    print()
    print("2022 Regular Season WP% Rankings")
    ranking(2022, 2022, regular_season_only=False, limit=None)
    # print()
    # print("Regular Season WP% Rankings (37 games)")
    # ranking(2009, 2022, regular_season_only=True, limit=39)
    # print()
    # print("Regular Season WP% Rankings (full)")
    # ranking(2009, 2022, regular_season_only=True, limit=None)
    # print()
    # print("Regular Season WP% Rankings (full & playoffs)")
    # ranking(2009, 2022, regular_season_only=False, limit=None)
    # print()


def ranking(start: int, end: int, regular_season_only: bool = True, limit: Union[int, None] = None) -> dict[str, dict]:
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
        x, t1 = getTeam(year, regular_season_only, limit)
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
            sums[team1 - 1][team2 - 1] += 1 - x[i]
            sums[team2 - 1][team1 - 1] += x[i]
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
            print("%s\t%6s" % ("TEAM", "WP%"))
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

def model(year: int, regular_season_only: bool = True, limit: Union[int, None] = None) -> dict[str, dict]:
    names1 = ["BC", "CGY", "EDM", "HAM", "MTL", "OTT", "SSK", "TOR", "WPG"]
    names2 = ["BC", "CGY", "EDM", "HAM", "MTL", "SSK", "TOR", "WPG"]
    store = {}
    for name in names1:
        store[name] = {}
    x, t1 = getTeam(year, regular_season_only, limit)
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
        sums[team1 - 1][team2 - 1] += 1 - x[i]
        sums[team2 - 1][team1 - 1] += x[i]
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


def getTeam(year: int, regular_season_only: bool = True, limit: Union[int, None] = None) \
        -> (list[(int, int)], list[(int, float)]):
    regular = ""
    if regular_season_only:
        regular = "AND event_type_id = 1"
    query = f'''
        SELECT team_1_team_id, team_2_team_id, team_1_wp, team_2_wp, games.game_id
        FROM epa LEFT JOIN games ON games.game_id = epa.game_id
        WHERE games.year = {year}
        {regular}
        ORDER BY games.game_id;
    '''
    results1 = store_helper.load_dataframe_from_query(query)
    results1['team_1_team_id'] = results1['team_1_team_id'].astype(int)
    results1['team_2_team_id'] = results1['team_2_team_id'].astype(int)
    results1.dropna(inplace=True)
    x = []
    gid = None
    te = []
    for index, row in results1.iterrows():
        t1 = int(row['team_1_team_id'])
        t2 = int(row['team_2_team_id'])
        t1wp = row['team_1_wp']
        t2wp = row['team_2_wp']
        gi = row['game_id']
        if gid is None or gid != gi:
            x.append([])
            te.append((t1, t2))
        gid = gi
        x[-1].append(t1wp)
        if limit is not None and len(te) == limit:
            break
    y = [sum(z) / len(z) for z in x]
    return y, te


if __name__ == "__main__":
    main()
