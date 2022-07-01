import os
import pickle
import time
import datetime

import config
import helper

MINUTE: int = 60


class DownloadHistory:
    __instance = None

    def __init__(self):
        self.request_history = []
        self.load()

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(DownloadHistory)
            return cls.__instance
        return cls.__instance

    def load(self) -> None:
        self.request_history.clear()
        if os.path.exists(config.API_HISTORY_FILE):
            with open(config.API_HISTORY_FILE, "rb") as file:
                self.request_history = pickle.load(file)

    def save(self) -> None:
        helper.mkdir(config.API_HISTORY_DIR)
        with open(config.API_HISTORY_FILE, "wb") as file:
            pickle.dump(self.request_history, file)

    def track(self, event: datetime) -> None:
        self.request_history.insert(0, event)

    def track_now(self) -> None:
        self.track(datetime.datetime.now())

    def wait(self) -> None:
        while len(self.request_history) >= config.API_MAX_CALLS_PER_MINUTE:
            while self.request_history and (datetime.datetime.now() - self.request_history[-1]).seconds > MINUTE:
                self.request_history.pop()
            if len(self.request_history) >= config.API_MAX_CALLS_PER_MINUTE:
                oldest: int = MINUTE - (datetime.datetime.now() - self.request_history[-1]).seconds
                time_to_expire: int = MINUTE - oldest
                print(f"Too many requests to API! {oldest}s since last request. Sleeping for {time_to_expire}s.")
                time.sleep(time_to_expire + 1)


download_history = DownloadHistory()
