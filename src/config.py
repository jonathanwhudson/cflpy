import datetime
import os
import pathlib

from urllib.parse import urljoin

API_KEY: str = "vrGdfJ8jdsWHN27QhqtlfVSIFtrhc7NI"
API_MAX_CALLS_PER_MINUTE: int = 30
API_HISTORY_DIR: str = pathlib.Path("temp")
API_HISTORY_FILE: str = API_HISTORY_DIR.joinpath("download_history.pkl")

URL_CFL: str = "https://api.cfl.ca/"
URL_V1: str = urljoin(URL_CFL, "v1/")
URL_v11: str = urljoin(URL_CFL, "v1.1/")
URL_GAMES: str = urljoin(URL_V1, "games/")

DIR_BASE: os.path = pathlib.Path("json")
DIR_GAMES: os.path = DIR_BASE.joinpath("games")

YEAR_CURRENT: int = datetime.datetime.now().date().year
YEAR_START_GAMES: int = 1958
YEAR_END_GAMES: int = YEAR_CURRENT
YEAR_START_ADV = 2004
YEAR_START_ADV_USEFUL = 2009
YEAR_END_ADV = YEAR_CURRENT
