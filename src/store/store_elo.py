import math

import pandas as pd

import config
from store import store_games, store_helper, store_columns


def load_elo_year_range(start: int, end: int) -> pd.DataFrame:
    """
    Load the Elo by processing games
    :param start: The first year
    :param end: The last year
    :return: Elo dataframe
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not loading any ELO as nothing in years=[{start},{end}]!")
    elo_df = load_elo(list(years), games=None)
    return elo_df


def load_elo_year(year: int) -> pd.DataFrame:
    """
    Load the Elo by processing games
    :param year: The year
    :return: Elo dataframe
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    elo_df = load_elo([year], games=None)
    return elo_df


def load_elo_games(games: set[int]) -> pd.DataFrame:
    """
    Load the Elo by processing games
    :param games: The games_ids
    :return: Elo dataframe
    """
    if type(games) != set:
        raise ValueError(f"Type of games <{type(games)}> should be set!")
    elo_df = load_elo(years=None, games=games)
    return elo_df


def load_elo_game(game_id: int) -> pd.DataFrame:
    """
    Load the Elo by processing games
    :param game_id: The game_id
    :return: Elo dataframe
    """
    if type(game_id) != int:
        raise ValueError(f"Type of game_id <{type(game_id)}> should be int!")
    elo_df = load_elo(years=None, games={game_id})
    return elo_df


def remove_elo_year_range(start: int, end: int) -> None:
    """
    Remove the elo
    :param start: The first year
    :param end: The last year
    :return: None
    """
    if type(start) != int:
        raise ValueError(f"Type of start <{type(start)}> should be int!")
    if type(end) != int:
        raise ValueError(f"Type of end <{type(end)}> should be int!")
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not removing any elo as nothing in years=[{start},{end}]!")
    games = store_games.extract_games_year_range(start, end)
    games = {game_id for (year, game_id) in games}
    remove_elo_games(games)


def remove_elo_year(year: int) -> None:
    """
    Remove the Elo
    :param year: The year
    :return: None
    """
    if type(year) != int:
        raise ValueError(f"Type of year <{type(year)}> should be int!")
    games = store_games.extract_games_year(year)
    games = {game_id for (year, game_id) in games}
    remove_elo_games(games)


def remove_elo_games(games: set[int]):
    """
    Remove the Elo
    :param games: The game_ids to remove
    :return: None
    """
    if type(games) != set:
        raise ValueError(f"Type of games <{type(games)}> should be set!")
    store_helper.execute(f'''DELETE FROM elo WHERE game_id in {str(tuple(games)).replace(",)", ")")}''')


def remove_elo_game(game_id: int):
    """
    Remove the Elo
    :param game_id: The game_id to remove
    :return: None
    """
    if type(game_id) != int:
        raise ValueError(f"Type of game_id <{type(game_id)}> should be int!")
    remove_elo_games({game_id})


def get_prev_elo(team_id, date_start, year):
    query = f"""SELECT games.team_1_team_id, games.year,games.team_2_team_id, team_1_elo_season_out,team_1_elo_franchise_out, team_2_elo_season_out,team_2_elo_franchise_out FROM elo LEFT JOIN games ON elo.game_id=games.game_id WHERE (games.team_1_team_id={team_id} OR games.team_2_team_id={team_id}) and games.date_start < \"{date_start}\" ORDER BY games.date_start"""
    elo_df = store_helper.load_dataframe_from_query(query)
    row_df = elo_df.tail(1)
    if row_df.empty:
        return 1500, 1500
    if row_df['team_1_team_id'].iat[0] == team_id:
        if row_df['year'].iat[0] != year:
            return 1500, (row_df['team_1_elo_franchise_out'].iat[0] - 1500.0) * (2.0 / 3.0) + 1500.0
        else:
            return row_df['team_1_elo_season_out'].iat[0], row_df['team_1_elo_franchise_out'].iat[0]
    else:
        if row_df['year'].iat[0] != year:
            return 1500, (row_df['team_2_elo_franchise_out'].iat[0] - 1500.0) * (2.0 / 3.0) + 1500.0
        else:
            return row_df['team_2_elo_season_out'].iat[0], row_df['team_2_elo_franchise_out'].iat[0]


def load_elo(years: list[int] = None, games: set[int] = None) -> pd.DataFrame:
    """
    Load Elo from database
    :param years: If years use list of years as basis
    :param games: If game_ids use list of game_ids as basis
    :return: A loaded dataframe
    """
    if not years and not games:
        raise ValueError(f"Not loading any Elo as no year or game_ids were given!")
    add = ""
    if years:
        years = set(years)
        add += f"games.year in {str(tuple(years)).replace(',)', ')')}"
    if years and games:
        add += " AND "
    if games:
        games = set(games)
        add += f"""games.game_id in {str(tuple(games)).replace(",)", ")")}"""
    query = f"""SELECT * from games WHERE {add} ORDER BY date_start"""
    elo_df = store_helper.load_dataframe_from_query(query)

    elo_season = {}
    elo_franchise = {}
    elo = []
    prev_year = None
    for index, game in elo_df.iterrows():
        team_1_team_id = game['team_1_team_id']
        team_2_team_id = game['team_2_team_id']
        date_start = game['date_start']
        year = game['year']
        possibly_regress = True
        if team_1_team_id not in elo_season:
            prev_elo_season, prev_elo_franchise = get_prev_elo(team_1_team_id, date_start, year)
            elo_season[team_1_team_id] = prev_elo_season
            elo_franchise[team_1_team_id] = prev_elo_franchise
            possibly_regress = False
        if team_2_team_id not in elo_season:
            prev_elo_season, prev_elo_franchise = get_prev_elo(team_2_team_id, date_start, year)
            elo_season[team_2_team_id] = prev_elo_season
            elo_franchise[team_2_team_id] = prev_elo_franchise
            possibly_regress = False
        if prev_year and prev_year != year and possibly_regress:
            for team, x in elo_season.items():
                elo_season[team] = 1500
            for team, elo_old in elo_franchise.items():
                elo_franchise[team] = (elo_old - 1500.0) * (2.0 / 3.0) + 1500.0
        played = game["event_status_id"] == 4
        actual_game = game["event_type_id"] >= 1
        if played and actual_game:
            elo1 = calc_elo_game(elo_season, game)
            elo_season[team_1_team_id] = elo1[1]
            elo_season[team_2_team_id] = elo1[3]
            elo2 = calc_elo_game(elo_franchise, game)
            elo_franchise[team_1_team_id] = elo2[1]
            elo_franchise[team_2_team_id] = elo2[3]
            elo.append([game['year'],game['game_id'], team_1_team_id,team_2_team_id]+elo1+elo2)
        prev_year = year
    elo_df = pd.DataFrame(elo, columns=["year","game_id","team_1_team_id","team_2_team_id", "team_1_elo_season_in", "team_1_elo_season_out", "team_2_elo_season_in", "team_2_elo_season_out", "team_1_elo_franchise_in", "team_1_elo_franchise_out", "team_2_elo_franchise_in", "team_2_elo_franchise_out"])
    store_helper.add_missing_columns(elo_df, store_columns.ELO)
    store_helper.ensure_type_columns(elo_df, store_columns.ELO)
    store_helper.reorder_columns(elo_df, store_columns.ELO)
    return elo_df


def reset_elo_all() -> None:
    """
    Store all Elo
    :return: Database will update Elo
    """
    elo_df = load_elo_year_range(config.YEAR_START_GAMES, config.YEAR_END_GAMES)
    remove_elo_year_range(config.YEAR_START_GAMES, config.YEAR_END_GAMES)
    store_helper.replace_dataframe(elo_df, "elo", datatype=store_columns.convert_to_sql_types(store_columns.ELO))


def reset_elo_year(year: int) -> None:
    """
    Store all Elo for year
    :param year: Year to update
    :return: Database will update Elo
    """
    elo_df = load_elo_year(year)
    remove_elo_year(year)
    store_helper.append_dataframe(elo_df, "elo", datatype=store_columns.convert_to_sql_types(store_columns.ELO))


def reset_elo_year_current() -> None:
    """
    Store all Elo for current year
    :return: Database will update Elo
    """
    reset_elo_year(config.YEAR_CURRENT)


def reset_elo_games(games: set[int]) -> None:
    """
    Store all Elo for games_ids
    :param games: games_ids to update
    :return: Database will update Elo
    """
    elo_df = load_elo_games(games)
    remove_elo_games(games)
    store_helper.append_dataframe(elo_df, "elo", datatype=store_columns.convert_to_sql_types(store_columns.ELO))


def reset_elo_active() -> None:
    """
    Store all Elo for active games
    :return: Database will update Elo
    """
    games = store_games.extract_games_active()
    games = {game_id for (year, game_id) in games}
    if games:
        reset_elo_games(games)


def reset_elo_game(game: int) -> None:
    """
    Store all Elo for game
    :param game: Game to update
    :return: Database will update Elo
    """
    elo_df = load_elo_game(game)
    remove_elo_game(game)
    store_helper.append_dataframe(elo_df, "elo", datatype=store_columns.convert_to_sql_types(store_columns.ELO))


def home_away(team_1_is_at_home, team_2_is_at_home):
    if team_1_is_at_home:
        return 65, -65
    if team_2_is_at_home:
        return -65, 65
    return 0, 0


def win(team_1_score, team_2_score):
    if team_1_score > team_2_score:
        return 1, 0
    if team_2_score > team_1_score:
        return 0, 1
    return 0.5, 0.5


def we(diff_1, diff_2):
    return 1 / (10 ** (-diff_1 / 550) + 1), 1 / (10 ** (-diff_2 / 550) + 1)


def win_lose_elo(elo_1_in, elo_2_in, team_1_score, team_2_score):
    if team_1_score > team_2_score:
        return elo_1_in, elo_2_in
    if team_2_score > team_1_score:
        return elo_2_in, elo_1_in
    return elo_1_in, elo_2_in


def calc(elo_1_in, elo_2_in, team_1_score, team_2_score, team_1_is_at_home, team_2_is_at_home):
    team_1_HA, team_2_HA = home_away(team_1_is_at_home, team_2_is_at_home)
    diff_1 = (elo_1_in - elo_2_in) + team_1_HA
    diff_2 = (elo_2_in - elo_1_in) + team_2_HA
    win_1, win_2 = win(team_1_score, team_2_score)
    we_1, we_2 = we(diff_1, diff_2)
    winner_elo, loser_elo = win_lose_elo(elo_1_in, elo_2_in, team_1_score, team_2_score)
    elo_change = 20 * math.log(abs(team_1_score - team_2_score) + 1) * (2.2 / ((winner_elo - loser_elo) * 0.001 + 2.2))
    elo_1_out = elo_1_in + (win_1 - we_1) * elo_change
    elo_2_out = elo_2_in + (win_2 - we_2) * elo_change
    return elo_1_out, elo_2_out


def calc_elo_game(store, game):
    team_1 = game["team_1_team_id"]
    team_2 = game["team_2_team_id"]
    team_1_score = game["team_1_score"]
    team_2_score = game["team_2_score"]
    team_1_is_at_home = game["team_1_is_at_home"]
    team_2_is_at_home = game["team_2_is_at_home"]
    elo_1_in = store[team_1]
    elo_2_in = store[team_2]
    elo_1_out, elo_2_out = calc(elo_1_in, elo_2_in, team_1_score, team_2_score, team_1_is_at_home, team_2_is_at_home)
    store[team_1] = elo_1_out
    store[team_2] = elo_2_out
    return [elo_1_in, elo_1_out, elo_2_in, elo_2_out]

def main() -> None:
    # Reset all elo
    if False:
        reset_elo_all()
    # Reset all elo for current year
    if True:
        reset_elo_year_current()
    # Reset all elo for chosen year
    if False:
        year = 2022
        reset_elo_year(year)
    # Reset all elo for certain game
    if False:
        game_ids = {6227, 6228, 6229}
        reset_elo_games(game_ids)
    # Reset elo for active games
    if False:
        reset_elo_active()
    # Reset all elo for certain game
    if False:
        game_id = 6229
        reset_elo_game(game_id)


if __name__ == '__main__':
    main()
    print("Done")

