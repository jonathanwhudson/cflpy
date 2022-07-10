import time

import config
import store
import download
import load


def main() -> None:
    print("Resetting (re-downloading) basic games")
    action_time = time.perf_counter()
    new_downloads_basic = download.download_games_basic(config.YEAR_START_GAMES, config.YEAR_END_GAMES, reset=True)
    print(f"Took {time.perf_counter() - action_time}s to run.")
    print(f"Years downloaded {len(new_downloads_basic)}")
    print("Loading basic games downloaded")
    games_df = load.load_games_basic_parsed(config.YEAR_START_GAMES, config.YEAR_END_GAMES, limit=new_downloads_basic)
    print(f"Games loaded {len(games_df)}")
    print("Determine all the games that occurred")
    action_time = time.perf_counter()
    games = load.extract_year_game_id_pairs(games_df)
    print(f"Took {time.perf_counter() - action_time}s to run.")
    print(f"Games extracted {len(games)}")
    print("Resetting (re-downloading) all advanced games")
    print(f"Expect to take at least {games // config.API_MAX_CALLS_PER_MINUTE} minutes")
    action_time = time.perf_counter()
    new_downloads_advanced = download.download_games_advanced(config.YEAR_START_ADV, config.YEAR_END_ADV, games,
                                                              reset=True)
    print(f"Took {time.perf_counter() - action_time}s to run.")
    print(f"Games downloaded {len(new_downloads_advanced)}")
    print("Loading advanced games downloaded")
    action_time = time.perf_counter()
    result = load.load_games_advanced_parsed(config.YEAR_START_ADV, config.YEAR_END_ADV, new_downloads_advanced)
    print(f"Took {time.perf_counter() - action_time}s to run.")
    print("Storing basic games")
    store.store_games_basic(games_df, store.IF_EXISTS_REPLACE)
    print("Storing advanced games")
    store.store_games_advanced(result, store.IF_EXISTS_REPLACE)


if __name__ == '__main__':
    main()
    print("Done")
