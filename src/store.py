import sqlite3

import pandas as pd

import config
import helper
import load
import column_dtypes

IF_EXISTS_FAIL = "fail"
IF_EXISTS_REPLACE = "replace"
IF_EXISTS_APPEND = "append"

TABLES_ADVANCED = []
for name in column_dtypes.TEAM_COLUMNS:
    TABLES_ADVANCED.append("team_" + name)
for name in column_dtypes.PLAYER_COLUMNS:
    TABLES_ADVANCED.append("players_" + name)
TABLES_ADVANCED += ['pbp', 'penalties', 'reviews', 'roster']


def store_games_basic(dataframe: pd.DataFrame, if_exists: str) -> None:
    store_dataframe(dataframe, "games", if_exists)


def store_games_advanced(results, if_exists: str) -> None:
    store_dataframe(results['pbp'], "pbp", if_exists)
    store_dataframe(results['penalties'], 'penalties', if_exists)
    store_dataframe(results['reviews'], "reviews", if_exists)
    store_dataframe(results['roster'], "roster", if_exists)
    for column_type, value in results['boxscore'].items():
        for column_name, df in value.items():
            store_dataframe(df, f"{column_type}_{column_name}", if_exists)


def store_dataframe(df: pd.DataFrame, table_name: str, if_exists: str = "fail") -> None:
    helper.mkdir(config.DB_DIR)
    connection = sqlite3.connect(config.DB_FILE)
    df.to_sql(table_name, connection, if_exists=if_exists, index=False)
    connection.close()


def remove_games_basic_years(start: int, end: int):
    years = set(range(start, end + 1, 1))
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(f"SELECT game_id FROM games WHERE year in {tuple(years)}", connection)
    connection.close()
    remove_games_basic(dataframe['game_id'].values.tolist())


def remove_games_basic_year(year: int):
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(f"SELECT game_id FROM games WHERE year={year}", connection)
    connection.close()
    remove_games_basic(dataframe['game_id'].values.tolist())


def remove_games_basic(game_ids: list[int]):
    connection = sqlite3.connect(config.DB_FILE)
    connection.execute(f'''DELETE FROM games WHERE game_id in {tuple(game_ids)}''')
    connection.commit()
    connection.close()


def remove_game_basic(game_id: int):
    connection = sqlite3.connect(config.DB_FILE)
    connection.execute(f'''DELETE FROM games WHERE game_id={game_id}''')
    connection.commit()
    connection.close()


def remove_games_advanced_years(start: int, end: int):
    years = set(range(start, end + 1, 1))
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(f"SELECT game_id FROM games WHERE year in {tuple(years)}", connection)
    connection.close()
    remove_games_advanced(dataframe['game_id'].values.tolist())


def remove_games_advanced_year(year: int):
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(f"SELECT game_id FROM games WHERE year={year}", connection)
    connection.close()
    remove_games_advanced(dataframe['game_id'].values.tolist())


def remove_games_advanced(game_ids: list[int]):
    connection = sqlite3.connect(config.DB_FILE)
    for table in TABLES_ADVANCED:
        x = f'''DELETE FROM {table} WHERE game_id in {str(tuple(game_ids)).replace(",)",")")}'''
        connection.execute(x)
        connection.commit()
    connection.close()


def remove_game_advanced(game_id: int):
    connection = sqlite3.connect(config.DB_FILE)
    for table in TABLES_ADVANCED:
        connection.execute(f'''DELETE FROM {table} WHERE game_id={game_id}''')
        connection.commit()
    connection.close()


def query(query_string: str):
    connection = sqlite3.connect(config.DB_FILE)
    dataframe = pd.read_sql(query_string, connection)
    connection.close()
    return dataframe


def main():
    games_df = load.load_games_basic_parsed(config.YEAR_START_GAMES, config.YEAR_END_GAMES)
    games = load.extract_year_game_id_pairs(games_df)
    store_games_basic(games_df, IF_EXISTS_REPLACE)
    result = load.load_games_advanced_parsed(config.YEAR_START_ADV, config.YEAR_END_ADV, games)
    store_games_advanced(result, IF_EXISTS_REPLACE)


if __name__ == '__main__':
    main()
    print("Done")
