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

REDOWNLOAD = False


def main() -> None:
    print("Resetting (re-downloading) basic games")
    years = download.download_games_basic(config.YEAR_START_GAMES, config.YEAR_END_GAMES, reset=REDOWNLOAD)
    print(f"Years downloaded {len(years)}")
    print("Resetting (storage) basic games downloaded")
    store_basic.reset_all_games()
    print("Determine all the games that occurred")
    games = store_advanced.extract_games()
    print(f"Games extracted {len(games)}")
    print("Resetting (re-downloading) all advanced games")
    print(f"Expect to take at least {len(games) // config.API_MAX_CALLS_PER_MINUTE} minutes")
    game_ids = download.download_games_advanced(config.YEAR_START_ADV, config.YEAR_END_ADV, games, reset=REDOWNLOAD)
    print(f"Games downloaded {len(game_ids)}")
    print("Resetting (storage) advanced games downloaded")
    store_advanced.reset_advanced_games_all()
    store_drives.reset_drives_all()
    store_epa.reset_epa_all()
    store_gei.reset_gei_all()


if __name__ == '__main__':
    main()
    print("Done")
