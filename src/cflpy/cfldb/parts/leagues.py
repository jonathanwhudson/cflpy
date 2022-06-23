import sys
import time

from cflpy.cfldb.cflconfig import config
from cflpy.cfldb.cfldb import cfldb

tables = ["leagues"]


def main() -> None:
    store_leagues()


def reset_leagues_tables() -> None:
    cfldb.reset_tables(tables)


def store_leagues() -> None:
    if config.reset_leagues:
        print("Storing leagues...")
        reset_leagues_tables()
        store_league(0, "Canadian Football League", "CFL")
        print("Storing leagues...Done")
    else:
        print("Use existing leagues.")


def store_league(league_id: int, league_name: str, league_abbr: str):
    print(f"Store league {league_abbr}")
    cfldb.action(f'''
        INSERT INTO leagues 
            (league_id, league_name, league_abbr) 
        VALUES 
            ({league_id},'{league_name}','{league_abbr}');
   ''')


if __name__ == "__main__":
    store = time.perf_counter()
    main()
    print(f"{sys.argv[0]} took {time.perf_counter() - store}s to run.")
