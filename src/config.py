import datetime
import os
import pathlib

from urllib.parse import urljoin

DIR_BASE: os.path = pathlib.Path("E:\\cflpy\\")

API_KEY: str = "vrGdfJ8jdsWHN27QhqtlfVSIFtrhc7NI"
API_MAX_CALLS_PER_MINUTE: int = 15 #30
API_HISTORY_DIR: os.path = DIR_BASE.joinpath("temp")
API_HISTORY_FILE: os.path = API_HISTORY_DIR.joinpath("download_history.pkl")

URL_CFL: str = "https://api.cfl.ca/"
URL_V1: str = urljoin(URL_CFL, "v1/")
URL_v11: str = urljoin(URL_CFL, "v1.1/")
URL_GAMES: str = urljoin(URL_V1, "games/")
URL_VENUES:str = urljoin(URL_V1, "teams/venues/")
URL_SEASONS:str = urljoin(URL_V1, "seasons/")
URL_TEAMS:str = urljoin(URL_V1, "teams/")
URL_STANDINGS:str = urljoin(URL_V1, "standings/")
URL_CROSSOVER:str = urljoin(URL_V1, "standings/crossover/")
URL_TRANSACTIONS:str = urljoin(URL_V1, "transactions/")
URL_PLAYERS:str = urljoin(URL_V1, "players/")

DIR_JSON: os.path = DIR_BASE.joinpath("json")
DIR_GAMES: os.path = DIR_JSON.joinpath("games")
FILE_VENUES: os.path = DIR_JSON.joinpath("venues.json")
FILE_SEASONS: os.path = DIR_JSON.joinpath("seasons.json")
FILE_TEAMS: os.path = DIR_JSON.joinpath("teams.json")
DIR_STANDINGS = DIR_JSON.joinpath("standings")
DIR_CROSSOVER = DIR_STANDINGS.joinpath("crossover")
DIR_TRANSACTIONS = DIR_JSON.joinpath("transactions")
DIR_PLAYERS = DIR_JSON.joinpath("players")

YEAR_CURRENT: int = datetime.datetime.now().date().year
YEAR_START_GAMES: int = 1958
YEAR_END_GAMES: int = YEAR_CURRENT
YEAR_START_ADV = 2004
YEAR_START_ADV_USEFUL = 2009
YEAR_END_ADV = YEAR_CURRENT
YEAR_CROSSOVER_STARTS = 1996
YEAR_TRANSACTIONS_STARTS = 1992
YEAR_PLAYERS_MIN_ROOKIE_YEAR = 1976
YEAR_PLAYERS_MAX_ROOKIE_YEAR = YEAR_CURRENT

DATA_DIR = DIR_BASE.joinpath("data")
DIR_DATABASE: os.path = DATA_DIR
FILE_DATABASE: os.path = DIR_DATABASE.joinpath("data.db")
DIR_MODEL: os.path = DATA_DIR
FILE_MODEL_EP: os.path = DIR_MODEL.joinpath("ep.mdl")
FILE_MODEL_WP: os.path = DIR_MODEL.joinpath("wp.mdl")
DIR_PLOT: os.path = DATA_DIR.joinpath("plots")

