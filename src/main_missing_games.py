import time

import config
import store
import download
import load
import store_advanced
import store_basic
import store_drives
import store_epa
import store_gei


def main() -> None:
    print("Resetting (re-downloading) missing games")
    years = download.download_games_basic(config.YEAR_START_GAMES, config.YEAR_END_GAMES, reset=False)
    print(f"Years downloaded {len(years)}")
    print("Resetting (storage) basic games downloaded")
    for year in years:
        store_basic.reset_games_year(year)
    print("Determine all the games that occurred")
    games = store_advanced.extract_games()
    print(f"Games extracted {len(games)}")
    print("Resetting (re-downloading) all advanced missing games")
    print(f"Expect to take at least {len(games) // config.API_MAX_CALLS_PER_MINUTE} minutes")
    game_ids = download.download_games_advanced(config.YEAR_START_ADV, config.YEAR_END_ADV, games, reset=False)
    print(f"Games downloaded {len(game_ids)}")
    if game_ids:
        print("Resetting (storage) advanced games downloaded for missing games")
        store_advanced.reset_advanced_games_games(game_ids)
        store_drives.reset_drives_games(game_ids)
        store_epa.reset_epa_games(game_ids)
        store_gei.reset_gei_games(game_ids)


if __name__ == '__main__':
    main()
    print("Done")
