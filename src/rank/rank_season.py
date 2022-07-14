import pandas as pd

from rank.rank_elo import model_elo
from rank.rank_score import model_score
from rank.rank_wp import model_wp

NAMES_8 = ["BC", "CGY", "EDM", "HAM", "MTL", "SSK", "TOR", "WPG"]
NAMES_9 = ["BC", "CGY", "EDM", "HAM", "MTL", "OTT", "SSK", "TOR", "WPG"]


def main() -> None:
    year = 2022
    regular_season_only = True
    print("Get Elo Rating Season")
    store_elo = model_elo(year=year, regular_season_only=regular_season_only, limit=None, season=True)
    print("Get Elo Rating Franchise")
    store_franchise = model_elo(year=year, regular_season_only=regular_season_only, limit=None, season=False)
    print("Get WP")
    store_wp = model_wp(year=year, regular_season_only=regular_season_only, limit=None)
    print("Get Score")
    store_score = model_score(year=year, regular_season_only=regular_season_only, limit=None)
    print("Combine")
    names = NAMES_9
    if 2006 <= year <= 2013:
        names = NAMES_8
    table = []
    for name in names:
        table.append([name, store_wp[name], store_score[name], store_elo[name], store_franchise[name]])
    data = pd.DataFrame(table, columns=["TEAM", "WP", "SCORE", "ELOS", "ELOF"])
    data['WP_RANK'] = data['WP'].rank(ascending=False).astype(int)
    data['SCORE_RANK'] = data['SCORE'].rank(ascending=False).astype(int)
    data['ELOS_RANK'] = data['ELOS'].rank(ascending=False).astype(int)
    data['ELOF_RANK'] = data['ELOF'].rank(ascending=False).astype(int)
    data['AVG'] = data[["WP", "SCORE", "ELOS", "ELOF"]].mean(axis=1)
    data['AVG_RANK'] = data[["WP_RANK", "SCORE_RANK", "ELOS_RANK", "ELOF_RANK"]].mean(axis=1)
    data1 = data.sort_values(["AVG"], ascending=False)
    print(data1[["TEAM", 'AVG', "WP", "SCORE", "ELOS", "ELOF"]])
    data2 = data.sort_values(["AVG_RANK"])
    print(data2[["TEAM", 'AVG_RANK', "WP_RANK", "SCORE_RANK", "ELOS_RANK", "ELOF_RANK"]])


if __name__ == '__main__':
    main()
    print("Done")
