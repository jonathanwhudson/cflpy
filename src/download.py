import backoff
import json
import os

import ratelimit
import requests
import time
import urllib

import config
import helper
import load
from download_history import download_history

MINUTE = 60


def download_games_basic(start: int, end: int, limit: set[int] = None, reset: bool = False) -> set[int]:
    years = set(range(start, end + 1, 1))
    if not years:
        raise ValueError(f"Not downloading anything as nothing in years=[{start},{end}]!")
    if limit and not limit.intersection(years):
        raise ValueError(f"Not downloading anything as nothing in years=[{start},{end}] due to limit={limit}!")
    downloaded = set()
    helper.mkdir(config.DIR_BASE)
    helper.mkdir(config.DIR_GAMES)
    for year in sorted(years):
        if not limit or year in limit:
            downloaded.update(download_games_basic_year(year, limit, reset))
    return downloaded


def download_games_basic_year(year: int, limit: set[int] = None, reset: bool = False) -> set[int]:
    if limit and year not in limit:
        raise ValueError(f"Not downloading anything as year={year} not in limit={limit}!")
    downloaded = set()
    url_year: str = urllib.parse.urljoin(config.URL_GAMES, str(year))
    dir_basic: os.path = config.DIR_GAMES.joinpath("basic")
    helper.mkdir(dir_basic)
    filename_year: os.path = dir_basic.joinpath(f"{year}.json")
    params: dict = {"key": config.API_KEY}
    if reset and os.path.exists(filename_year):
        os.remove(filename_year)
    if not os.path.exists(filename_year):
        download(url_year, filename_year, params)
        downloaded.add(year)
    while check_errors(filename_year):
        print(f"Error in {filename_year} re-downloading!")
        os.remove(filename_year)
        download(url_year, filename_year, params)
        downloaded.add(year)
    return downloaded


def download_games_advanced(start: int, end: int, limit: set[int, int], reset: bool = False) -> set[(int, int)]:
    years = set(range(start, end + 1, 1))
    limit_years = {year for (year, game_id) in limit}
    if not years:
        raise ValueError(f"Not downloading anything as nothing in years=[{start},{end}]!")
    if not limit_years.intersection(years):
        raise ValueError(
            f"Not downloading anything as nothing in years=[{start},{end}] due to limit_years={limit_years}!")
    downloaded = set()
    helper.mkdir(config.DIR_BASE)
    helper.mkdir(config.DIR_GAMES)
    for year in years:
        if year in limit_years:
            downloaded.update(download_games_advanced_year(year, limit, reset))
    return downloaded


def download_games_advanced_year(year: int, limit: set[int, int], reset: bool = False) -> set[(int, int)]:
    limit_years = {year for (year, game_id) in limit}
    if year not in limit_years:
        raise ValueError(f"Not downloading anything as year={year} not in limit_years={limit_years}!")
    downloaded = set()
    for (game_year, game_id) in sorted(sorted(limit, key=lambda x: x[1]), key=lambda x: x[0]):
        if game_year == year:
            downloaded.update(download_game_advanced(year, game_id, limit, reset))
    return downloaded


def download_game_advanced(year: int, game_id: int, limit: set[int, int], reset: bool = False) -> set[(int, int)]:
    if (year, game_id) not in limit:
        raise ValueError(f"Not downloading anything as ({year},{game_id}) not in limit={limit}!")
    downloaded = set()
    dir_advanced: os.path = config.DIR_GAMES.joinpath("advanced")
    helper.mkdir(dir_advanced)
    dir_year: os.path = dir_advanced.joinpath(str(year))
    helper.mkdir(dir_year)
    filename_game: os.path = dir_year.joinpath(f"{game_id}.json")
    url_year: str = urllib.parse.urljoin(config.URL_GAMES, str(year) + "/")
    url_game: str = urllib.parse.urljoin(url_year, f"game/{game_id}/")
    params: dir = {"key": config.API_KEY, "include": "boxscore,play_by_play,rosters,penalties,play_reviews"}
    if reset and os.path.exists(filename_game):
        os.remove(filename_game)
    if not os.path.exists(filename_game):
        download(url_game, filename_game, params)
        downloaded.add((year, game_id))
    # Slows things down significantly and likely only applies to error in API request which we should not be making
    # while check_errors(filename_game):
    #    os.remove(filename_game)
    #    download(url_game, filename_game, params)
    #    downloaded.add((year, game_id))
    return downloaded


def download(url: str, filename: os.path, params: dict) -> None:
    download_history.load()
    full_url: str = f"{url}?"
    for key, value in params.items():
        full_url += f"{key}={value},"
    while True:
        print(f"Downloading:{full_url}")
        download_history.wait()
        download_history.track_now()
        download_history.save()
        response: requests.Response = download_limited(url, params)
        if response.status_code == requests.codes.ok:
            with open(filename, "w") as file:
                if response.text:
                    file.write(response.text)
                    return
        if response.status_code == 429:
            print(f"API responded 'Too Many Requests' (429) sleep {MINUTE}s and retry!")
            time.sleep(MINUTE)
        else:
            print(f"API responded with failure code ({response.status_code}) sleep {MINUTE}s and retry!")
            time.sleep(MINUTE)


def backoff_handler(details):
    url = details['args'][0]
    full_url = f"{url}?"
    for key, value in details['args'][1].items():
        full_url += f"{key}={value},"
    wait = details['wait']
    tries = details['tries']
    print(f"Backing off:{full_url} {wait:3.1f}s after {tries}.")


@backoff.on_exception(backoff.constant, ratelimit.RateLimitException, max_tries=5, on_backoff=backoff_handler,
                      jitter=None, interval=15)
@ratelimit.limits(calls=config.API_MAX_CALLS_PER_MINUTE, period=MINUTE)
def download_limited(url: str, params: dict[str, str]) -> requests.Response:
    return requests.get(url, params=params)


def check_errors(filename: os.path) -> bool:
    with open(filename) as file:
        text = file.read()
    if not text:
        print(f"Nothing in {filename}!")
        return True
    json_string = json.loads(text)
    errors = json_string['errors']
    if errors:
        print(f"Error in json 'errors' category for {filename}!")
        return True
    return False


if __name__ == '__main__':
    new_downloads = download_games_basic(config.YEAR_START_GAMES, config.YEAR_CURRENT, reset=False)
    new_downloads.update(download_games_basic(config.YEAR_CURRENT, config.YEAR_CURRENT, reset=True))
    games_df = load.load_games_basic_parsed(config.YEAR_START_GAMES, config.YEAR_END_GAMES, limit=new_downloads)
    games = load.extract_year_game_id_pairs(games_df)
    new_downloads = download_games_advanced(config.YEAR_START_ADV, config.YEAR_END_ADV, games, reset=False)
    print("Done")
