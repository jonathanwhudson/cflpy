import time

import config
import store
import download
import load


def main() -> None:
    print("Download any missing basic games")
    action_time = time.perf_counter()
    new_downloads_basic = download.download_games_basic(config.YEAR_START_GAMES, config.YEAR_END_GAMES, reset=False)
    print(f"Took {time.perf_counter() - action_time}s to run.")
    print(f"Years downloaded {len(new_downloads_basic)}")
    if new_downloads_basic:
        print("Loading basic games downloaded")
        games_df = load.load_games_basic_parsed(config.YEAR_START_GAMES, config.YEAR_END_GAMES, new_downloads_basic)
        print(f"Games loaded {len(games_df)}")
        print("Removing basic games from DB")
        for year in new_downloads_basic:
            store.remove_games_basic_year(year)
        print("Storing basic games")
        store.store_games_basic(games_df, store.IF_EXISTS_APPEND)
    print("Download any missing advanced games")
    print("Loading basic games for all years")
    action_time = time.perf_counter()
    games_df_temp = load.load_games_basic_parsed(config.YEAR_START_GAMES, config.YEAR_END_GAMES)
    print(f"Took {time.perf_counter() - action_time}s to run.")
    print(f"Games loaded {len(games_df_temp)}")
    print("Determine all the games that occurred")
    games_temp = load.extract_year_game_id_pairs(games_df_temp)
    print(f"Games extracted {len(games_temp)}")
    print("Download missing advanced games")
    print(f"Expect to take at max {len(games_temp)//config.API_MAX_CALLS_PER_MINUTE} minutes")
    action_time = time.perf_counter()
    new_downloads_advanced = download.download_games_advanced(config.YEAR_START_ADV, config.YEAR_END_ADV, games_temp,
                                                              reset=False)
    print(f"Took {time.perf_counter() - action_time}s to run.")
    print(f"Games downloaded {len(new_downloads_advanced)}")
    if new_downloads_advanced:
        print("Loading advanced games downloaded")
        action_time = time.perf_counter()
        result = load.load_games_advanced_parsed(config.YEAR_START_ADV, config.YEAR_END_ADV, new_downloads_advanced)
        print(f"Took {time.perf_counter() - action_time}s to run.")
        print("Removing advanced games for current year from DB")
        store.remove_games_advanced([year for (year, game_id) in new_downloads_advanced])
        print("Storing advanced games")
        store.store_games_advanced(result, store.IF_EXISTS_APPEND)


if __name__ == '__main__':
    main()
    print("Done")
