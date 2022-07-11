import config
import download
import store_advanced
import store_basic
import store_drives
import store_epa
import store_gei


def main() -> None:
    download.download_games_basic(config.YEAR_CURRENT, config.YEAR_CURRENT, reset=False)
    store_basic.reset_games_current_year()
    games_active = store_advanced.extract_games_active()
    download.download_games_basic(config.YEAR_CURRENT, config.YEAR_CURRENT, reset=True)
    store_basic.reset_games_current_year()
    if games_active:
        game_ids = download.download_games_advanced(config.YEAR_CURRENT, config.YEAR_CURRENT, games_active,
                                                    reset=True)
        if game_ids:
            store_advanced.reset_advanced_games_games(game_ids)
            store_drives.reset_drives_games(game_ids)
            store_epa.reset_epa_games(game_ids)
            store_gei.reset_gei_games(game_ids)


if __name__ == '__main__':
    main()
    print("Done")
