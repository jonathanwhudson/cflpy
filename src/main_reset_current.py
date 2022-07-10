import time

import config
import download
import load
import store


def main() -> None:
    print("Resetting (re-downloading) basic games for current year")
    action_time = time.perf_counter()
    new_downloads_basic = download.download_games_basic(config.YEAR_CURRENT, config.YEAR_CURRENT, reset=True)
    print(f"Took {time.perf_counter() - action_time}s to run.")
    print(f"Years downloaded {len(new_downloads_basic)}")
    print("Loading basic games downloaded for current year")
    games_df = load.load_games_basic_parsed(config.YEAR_CURRENT, config.YEAR_CURRENT, limit=new_downloads_basic)
    print(f"Games loaded {len(games_df)}")
    print("Determine all the games that occurred for current year")
    games = load.extract_year_game_id_pairs(games_df)
    print(f"Games extracted {len(games)}")
    print("Resetting (re-downloading) all advanced games for current year")
    print(f"Expect to take at least {len(games) // config.API_MAX_CALLS_PER_MINUTE} minutes")
    action_time = time.perf_counter()
    new_downloads_advanced = download.download_games_advanced(config.YEAR_CURRENT, config.YEAR_CURRENT, games,
                                                              reset=True)
    print(f"Took {time.perf_counter() - action_time}s to run.")
    print(f"Games downloaded {len(new_downloads_advanced)}")
    print("Loading advanced games downloaded for current year")
    result = load.load_games_advanced_parsed(config.YEAR_CURRENT, config.YEAR_CURRENT, new_downloads_advanced)
    print("Removing basic games for current year from DB")
    store.remove_games_basic_year(config.YEAR_CURRENT)
    print("Removing advanced games for current year from DB")
    store.remove_games_advanced_year(config.YEAR_CURRENT)
    print("Storing basic games")
    store.store_games_basic(games_df, store.IF_EXISTS_APPEND)
    print("Storing advanced games")
    store.store_games_advanced(result, store.IF_EXISTS_APPEND)


if __name__ == '__main__':
    main()
    print("Done")
