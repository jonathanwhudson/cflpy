import config
import download
import load

def main() -> None:
    games_df = load.load_games_basic_parsed(config.YEAR_CURRENT, config.YEAR_CURRENT)
    games = load.extract_year_game_id_pairs_active(games_df)
    new_downloads = download.download_games_basic(config.YEAR_CURRENT, config.YEAR_CURRENT, reset=True)
    games_df = load.load_games_basic_parsed(config.YEAR_CURRENT, config.YEAR_CURRENT, limit=new_downloads)
    if games:
        new_downloads = download.download_games_advanced(config.YEAR_CURRENT, config.YEAR_CURRENT, games, reset=True)
        result = load.load_games_advanced_parsed(config.YEAR_CURRENT, config.YEAR_CURRENT, new_downloads)

if __name__ == '__main__':
    main()
    print("Done")
