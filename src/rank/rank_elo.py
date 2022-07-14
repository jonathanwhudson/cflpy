from typing import Union

import pandas as pd

from store import store_helper

NAMES_8 = ["BC", "CGY", "EDM", "HAM", "MTL", "SSK", "TOR", "WPG"]
NAMES_9 = ["BC", "CGY", "EDM", "HAM", "MTL", "OTT", "SSK", "TOR", "WPG"]


def main() -> None:
    store = model_elo(2022, regular_season_only=False, limit=None, season=True)
    for team, score in sorted(store.items(), key=lambda x: x[1], reverse=True):
        print(f"{team:4s}\t{score:.4f}")
    print()
    store = model_elo(2022, regular_season_only=False, limit=None, season=False)
    for team, score in sorted(store.items(), key=lambda x: x[1], reverse=True):
        print(f"{team:4s}\t{score:.4f}")


def model_elo(year: int, regular_season_only: bool = True, limit: Union[int, None] = None, season: bool = True) -> dict[
    str, dict]:
    names = NAMES_9
    if 2006 <= year <= 2013:
        names = NAMES_8
    store = get_elo(year, regular_season_only, limit, season)
    result = [0] * len(names)
    for team, elo in store.items():
        if 2006 <= year <= 2013 and team >= 6:
            team -= 1
        result[int(team) - 1] = elo
    zipped = zip(names, result)
    total = sum(result)
    store = {}
    for team, value in zipped:
        store[team] = value / total * len(names) / 2
    return store


def get_elo(year: int, regular_season_only: bool = True, limit: Union[int, None] = None,
            season: bool = True) -> pd.DataFrame:
    if regular_season_only:
        regular = "AND event_type_id = 1"
    else:
        regular = "AND event_type_id >= 1"
    if season:
        query = f'''SELECT elo.team_1_team_id, elo.team_2_team_id, elo.team_1_elo_season_out, elo.team_2_elo_season_out, games.game_id FROM elo LEFT JOIN games ON games.game_id = elo.game_id WHERE games.year = {year} {regular} AND event_status_id=4 ORDER BY games.date_start'''
    else:
        query = f'''SELECT elo.team_1_team_id, elo.team_2_team_id, elo.team_1_elo_franchise_out, elo.team_2_elo_franchise_out, games.game_id FROM elo LEFT JOIN games ON games.game_id = elo.game_id WHERE games.year = {year} {regular} AND event_status_id=4 ORDER BY games.date_start'''
    dataframe = store_helper.load_dataframe_from_query(query)
    if limit:
        dataframe = dataframe.head(limit)
    store = {}
    for i, row in dataframe.iterrows():
        if season:
            store[row['team_1_team_id']] = row['team_1_elo_season_out']
            store[row['team_2_team_id']] = row['team_2_elo_season_out']
        else:
            store[row['team_1_team_id']] = row['team_1_elo_franchise_out']
            store[row['team_2_team_id']] = row['team_2_elo_franchise_out']
    return store


if __name__ == "__main__":
    main()
    print("Done")
