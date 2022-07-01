import config
import download
import load

def main() -> None:
    new_downloads = download.download_games_basic(config.YEAR_START_GAMES, config.YEAR_END_GAMES, reset=False)
    games_df = load.load_games_basic_parsed(config.YEAR_START_GAMES, config.YEAR_END_GAMES, limit=new_downloads)
    games = load.extract_year_game_id_pairs(games_df)
    new_downloads = download.download_games_advanced(config.YEAR_START_ADV, config.YEAR_END_ADV, games, reset=False)
    result = load.load_games_advanced_parsed(config.YEAR_START_ADV, config.YEAR_END_ADV, new_downloads)

if __name__ == '__main__':
    main()
    print("Done")
