import sys
import time

from cflpy.cfldb.cflconfig import config
from cflpy.cfldb.cfldb import cfldb
from cflpy.cfldb.cfldown import get_seasons

tables = ["seasons"]


def main() -> None:
    store_seasons()


def reset_seasons_tables() -> None:
    cfldb.reset_tables(tables)


def drop_standings_year(year: int) -> None:
    cfldb.action(f'''DELETE FROM seasons WHERE year = {year};''')


def store_seasons() -> None:
    if config.reset_seasons or config.reset_current_season:
        print("Storing seasons...")
        if config.reset_seasons:
            reset_seasons_tables()
        elif config.reset_current_season:
            drop_standings_year(config.curr_year)
        seasons = get_seasons()
        for season in seasons['seasons']:
            year = season['season']
            if config.reset_seasons or (config.reset_current_season and year == config.curr_year):
                print(f"Store season {year}.")
                season_types = list(season.keys())
                season_types.remove('season')
                for season_type in season_types:
                    info = season[season_type]
                    start = info['start']
                    end = info['end']
                    store_season("0", year, season_type, start, end)
        print("Storing seasons...Done")
    else:
        print("Use existing seasons.")


def store_season(league_id: str, year: str, season_type: str, start: str, end: str) -> None:
    if start != "" and end != "":
        query = f'''
            INSERT INTO seasons 
                (year, league_id, type, season_start, season_end)
            VALUES 
                ({year},{league_id},'{season_type}','{start}','{end}');
       '''
        cfldb.action(query)


if __name__ == "__main__":
    store = time.perf_counter()
    main()
    print(f"{sys.argv[0]} took {time.perf_counter() - store}s to run.")
