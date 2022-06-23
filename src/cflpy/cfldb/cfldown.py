import datetime
import json
import os
from typing import Any

import requests
from requests import Response

from cflpy.cfldb.cflconfig import config
from cflpy.cfldb.cfldb import cfldb
from cflpy.cfldb.queue import queue


def get_data(url: str, file: str, use_cache: bool = True) -> Any:
    if not os.path.exists(file) or not use_cache:
        r = get_request(url, config.request_params)
        write_file(file, r.text)
        queue.save()
        return json.loads(r.text)['data']
    else:
        return json.loads(read_file(file))['data']


def get_request(url: str, payload: dict[str, str]) -> Response:
    print("Downloading: " + url)
    queue.wait()
    queue.track(datetime.datetime.now())
    r = requests.get(url, params=payload)
    if r.status_code == requests.codes.ok:
        return r
    else:
        r.raise_for_status()


def write_file(filename: str, text: str) -> None:
    with open(filename, "w") as file:
        file.write(text)


def read_file(filename: str) -> str:
    with open(filename, "r") as file:
        return file.readlines()[0].strip()


def get_seasons() -> dict[str, list[dict[str, Any]]]:
    seasons_url = config.base_url_v10 + "seasons"
    seasons_file = config.down_base_file + "seasons" + ".json"
    use_cache = True
    if config.download_previous_years or config.download_current_year:
        use_cache = False
    return get_data(seasons_url, seasons_file, use_cache)


def get_venues() -> list[dict[str, Any]]:
    venues_url = config.base_url_v10 + "teams/venues"
    venues_file = config.down_base_file + "venues" + ".json"
    use_cache = True
    if config.download_previous_years or config.download_current_year:
        use_cache = False
    return get_data(venues_url, venues_file, use_cache)


def get_teams() -> list[dict[str, Any]]:
    teams_url = config.base_url_v10 + "teams"
    teams_file = config.down_base_file + "teams" + ".json"
    use_cache = True
    if config.download_previous_years or config.download_current_year:
        use_cache = False
    return get_data(teams_url, teams_file, use_cache)


def get_standings() -> dict[int, dict[str, dict[str, Any]]]:
    stand_url = config.base_url_v10 + "standings/"
    cross_url = config.base_url_v10 + "standings/crossover/"
    data = {}
    for year in range(config.standings_start_year, config.curr_year + 1):
        use_cache = True
        if config.download_previous_years or (config.download_current_year and config.curr_year == year):
            use_cache = False
        stand_year_file = config.stand_file + str(year) + ".json"
        standings = get_data(stand_url + str(year), stand_year_file, use_cache)
        cross_year_file = config.stand_file + str(year) + "-crossover.json"
        crossover = get_data(cross_url + str(year), cross_year_file, use_cache)
        data[year] = {'standings': standings, 'crossover': crossover}
    return data


def get_players() -> list[dict[str, Any]]:
    player_id = set()
    players = []
    page = 1
    while True:
        players_file = config.play_file + str(page) + ".json"
        players_url = config.base_url_v10 + "players/?page[number]=" + str(page) + "&page[size]=100"
        use_cache = True
        if config.download_previous_years:
            use_cache = False
        data = get_data(players_url, players_file, use_cache)
        for player in data:
            if player['cfl_central_id'] in player_id:
                continue
            else:
                players.append(player)
                player_id.add(player['cfl_central_id'])
        if len(data) < 100:
            break
        page += 1
    page = 1
    while True:
        year = config.curr_year
        players_year_dir = config.play_file + str(year) + os.sep
        if not os.path.exists(players_year_dir):
            os.mkdir(players_year_dir)
        players_file = players_year_dir + str(page) + ".json"
        players_url = config.base_url_v10 + "players/?page[number]=" + str(
            page) + "&page[size]=100&sort=cfl_central_id&filter[rookie_year][eq]=" + str(year)
        use_cache = True
        if config.download_previous_years or (config.download_current_year and config.curr_year == year) or (config.reset_new_players and config.curr_year == year):
            use_cache = False
        data = get_data(players_url, players_file, use_cache)
        for player in data:
            if player['cfl_central_id'] in player_id:
                continue
            else:
                players.append(player)
                player_id.add(player['cfl_central_id'])
        if len(data) < 100:
            break
        page += 1
    return players


def get_games(start_year: int = None, end_year: int = None) -> list[dict[str, Any]]:
    query = """
    SELECT game_id
    FROM status
    WHERE event_status_id = 4
    ORDER BY game_id;"""
    games = []
    start = config.games_start_year
    end = config.curr_year
    if start_year is not None:
        start = start_year
    if end_year is not None:
        end = end_year
    stored_played_games = cfldb.fetchall(query)
    stored_played_games = [x[0] for x in stored_played_games]
    use_cache = True
    for year in range(start, end + 1):
        print(f"Downloading games...for year {year}")
        games_url = config.base_url_v10 + "games/" + str(year)
        games_year_file = config.games_file + str(year) + ".json"
        if config.download_previous_years or (config.download_current_year and year == config.curr_year) or (config.download_new_games and year == config.curr_year):
            use_cache = False
        data = get_data(games_url, games_year_file, use_cache)
        played_games = []
        for game in data:
            if game["event_status"]["event_status_id"] == 4:
                played_games.append(game["game_id"])
        for game in data:
            use_cache = True
            if config.download_previous_years or (config.download_current_year and year == config.curr_year) or (config.download_new_games and year == config.curr_year):
                use_cache = False
            game_id = game['game_id']
            if game_id in stored_played_games:
                use_cache = True
            elif game_id not in played_games:
                use_cache = True
            url = config.base_url_v10 + "games/" + str(year) + "/game/" + str(game_id) + "?include=boxscore,play_by_play,rosters,penalties,play_reviews"
            game_dir = config.games_file + str(year)
            if not os.path.exists(game_dir):
                os.mkdir(game_dir)
            file = game_dir + os.sep + str(game_id) + ".json"
            games.extend(get_data(url, file, use_cache))
    return games
