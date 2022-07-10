import config
import store
import download
import load

def main() -> None:
    # Load current games and figure out which are active
    download.download_games_basic(config.YEAR_CURRENT, config.YEAR_CURRENT, reset=False)
    games_df_active = load.load_games_basic_parsed(config.YEAR_CURRENT, config.YEAR_CURRENT)
    games_active = load.extract_year_game_id_pairs_active(games_df_active)
    # Reset the current year in case it changed (we do this after previous as if we did it before active games would be final and we'd ignore them)
    new_downloads_basic = download.download_games_basic(config.YEAR_CURRENT, config.YEAR_CURRENT, reset=True)
    # Load the games from the current year
    games_df = load.load_games_basic_parsed(config.YEAR_CURRENT, config.YEAR_CURRENT, limit=new_downloads_basic)
    store.remove_games_basic_year(config.YEAR_CURRENT)
    store.store_games_basic(games_df, store.IF_EXISTS_APPEND)
    # If there were active games we will retrieve them
    if games_active:
        new_downloads_advanced = download.download_games_advanced(config.YEAR_CURRENT, config.YEAR_CURRENT, games_active, reset=True)
        if new_downloads_advanced:
            result = load.load_games_advanced_parsed(config.YEAR_CURRENT, config.YEAR_CURRENT, new_downloads_advanced)
            store.remove_games_advanced([year for (year, game_id) in new_downloads_advanced])
            store.store_games_advanced(result, store.IF_EXISTS_APPEND)

if __name__ == '__main__':
    main()
    print("Done")
