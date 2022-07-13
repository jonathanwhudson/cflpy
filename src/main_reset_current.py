import config
import download
import store_advanced
import store_basic
import store_drives
import store_epa
import store_gei

REDOWNLOAD = False

def main() -> None:
    print("Resetting (re-downloading) basic games for current year")
    years = download.download_games_basic(config.YEAR_CURRENT, config.YEAR_CURRENT, reset=REDOWNLOAD)
    print(f"Years downloaded {len(years)}")
    print("Resetting (storage) basic games downloaded for current year")
    store_basic.reset_games_current_year()
    print("Determine all the games that occurred for current year")
    games = store_advanced.extract_games_current_year()
    print(f"Games in current year {len(games)}")
    print("Resetting (re-downloading) all advanced games for current year")
    print(f"Expect to take at least {len(games) // config.API_MAX_CALLS_PER_MINUTE} minutes")
    game_ids = download.download_games_advanced(config.YEAR_CURRENT, config.YEAR_CURRENT, games, reset=REDOWNLOAD)
    print(f"Games downloaded {len(game_ids)}")
    print("Resetting (storage) advanced games downloaded for current year")
    store_advanced.reset_advanced_games_current_year()
    store_drives.reset_drives_year_current()
    store_epa.reset_epa_year_current()
    store_gei.reset_gei_year_current()


if __name__ == '__main__':
    main()
    print("Done")
