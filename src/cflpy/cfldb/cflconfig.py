import os
import dotenv


class Config:
    instance = None  # type: Config

    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super().__new__(Config)
            return cls.instance
        return cls.instance

    def __init__(self):
        dotenv.load_dotenv()

        # Behaviour
        self.curr_year = 2022  # type: int
        self.download_previous_years = False  # type: bool
        self.download_current_year = False  # type: bool
        self.download_new_games = True  # type: bool
        self.reset_leagues = False  # type: bool
        self.reset_seasons = False  # type: bool
        self.reset_current_season = False  # type: bool
        self.reset_venues = False  # type: bool
        self.reset_teams = False  # type: bool
        self.reset_standings = False  # type: bool
        self.reset_current_standings = True  # type: bool
        self.reset_players = False  # type: bool
        self.reset_new_players = True  # type: bool
        self.reset_previous_games = False  # type: bool
        self.reset_current_games = False  # type: bool
        self.reset_new_games = True  # type: bool
        self.reset_elo = True  # type: bool

        # CFLDB CONFIG
        self.database = "postgres"  # type: str
        self.user = os.getenv("POSTGRES_USER")  # type: str
        self.password = os.getenv("POSTGRES_PASSWORD")  # type: str
        self.host = os.getenv("POSTGRES_HOST")  # type: str
        self.port = os.getenv("POSTGRES_PORT")  # type: int
        self.db_name = os.getenv("POSTGRES_DB")  # type: str
        self.min_pool_size = 1  # type: int
        self.max_pool_size = 12  # type: int

        # DownloadQueue Config
        self.rate = 15  # type: int
        self.history_base_file = f"E:{os.sep}API{os.sep}data{os.sep}temp{os.sep}"  # type: str
        self.request_history_file = self.history_base_file + "request_history.pickle"  # type: str
        if not os.path.exists(self.history_base_file):
            os.mkdir(self.history_base_file)

        # Downloader Config
        self.standings_start_year = 1996  # type: int
        self.games_start_year = 2004  # type: int
        self.transactions_start_year = 2004  # type: int
        self.key = os.getenv("CFL_API_KEY")  # type: str
        self.request_params = {"key": self.key}  # type:  dict
        self.base_url_v10 = "http://api.cfl.ca/v1/"  # type: str
        self.base_url_v11 = "http://api.cfl.ca/v1.1/"  # type: str
        self.down_base_file = f"E:{os.sep}API{os.sep}data{os.sep}"  # type: str
        self.stand_file = self.down_base_file + f"standings{os.sep}"  # type: str
        self.games_file = self.down_base_file + f"games{os.sep}"  # type: str
        self.trans_file = self.down_base_file + f"transactions{os.sep}"  # type: str
        self.play_file = self.down_base_file + f"players{os.sep}"  # type: str
        if not os.path.exists(self.down_base_file):
            os.mkdir(self.down_base_file)
        if not os.path.exists(self.stand_file):
            os.mkdir(self.stand_file)
        if not os.path.exists(self.games_file):
            os.mkdir(self.games_file)
        if not os.path.exists(self.trans_file):
            os.mkdir(self.trans_file)
        if not os.path.exists(self.play_file):
            os.mkdir(self.play_file)


config = Config()
