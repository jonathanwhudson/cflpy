import os
import pickle
import time
import datetime

import config
from download import download_helper

MINUTE = 60
"""Amount of seconds in a minute"""


class DownloadHistory:
    """
    A class to handle storage of datetime timestamps of API requests, so that it can be limited
    """
    __instance = None
    """Singleton design pattern, only one DownloadHistory"""

    def __init__(self):
        """
        Create DownloadHistory with empty requests, load in existing requests if they exist
        """
        self.request_history = []
        self.load()

    def __new__(cls, *args, **kwargs):
        """
        When file is loaded then Singleton is set up
        :param args:
        :param kwargs:
        """
        if cls.__instance is None:
            cls.__instance = super().__new__(DownloadHistory)
            return cls.__instance
        return cls.__instance

    def load(self) -> None:
        """
        Load and replace existing request history from pickled storage
        :return: None
        """
        self.request_history.clear()
        if os.path.exists(config.API_HISTORY_FILE):
            with open(config.API_HISTORY_FILE, "rb") as file:
                self.request_history = pickle.load(file)
                self.update()

    def save(self) -> None:
        """
        Save existing request history to pickled storage
        :return: None
        """
        download_helper.mkdir(config.API_HISTORY_DIR)
        with open(config.API_HISTORY_FILE, "wb") as file:
            self.update()
            pickle.dump(self.request_history, file)

    def track(self, event: datetime) -> None:
        """
        Track a request to API
        :param event: The event to track
        :return: None
        """
        if type(event) != datetime.datetime:
            raise ValueError(f"Type of event <{type(event)}> should be datetime!")
        self.request_history.insert(0, event)
        self.update()

    def track_now(self) -> None:
        """
        Track event occurring now
        :return: None
        """
        self.track(datetime.datetime.now())

    def update(self) -> None:
        """
        Update request history, by dropping anything outside a minute range
        :return: None
        """
        while self.request_history and (datetime.datetime.now() - self.request_history[-1]).seconds > MINUTE:
            self.request_history.pop()

    def wait(self) -> None:
        """
        Wait on request history becoming open
        :return: None
        """
        while len(self.request_history) >= config.API_MAX_CALLS_PER_MINUTE:
            self.update()
            if len(self.request_history) >= config.API_MAX_CALLS_PER_MINUTE:
                oldest: int = (datetime.datetime.now() - self.request_history[-1]).seconds
                time_to_expire: int = MINUTE - oldest
                print(
                    f"Too many requests ({len(self.request_history)}) to API! {oldest}s since oldest request. Sleeping for {time_to_expire + 1}s.")
                time.sleep(time_to_expire + 1)

    def size(self) -> int:
        """
        Get current size of request history
        :return:
        """
        self.update()
        return len(self.request_history)


# This is the download_history we will import and use elsewhere
download_history = DownloadHistory()
