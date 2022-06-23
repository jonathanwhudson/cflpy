import datetime
import os
import pickle
import time

from cflpy.cfldb.cflconfig import config


class DownloadQueue:
    instance = None  # type: DownloadQueue

    def __init__(self):
        self.request_history = []
        self.load()

    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super().__new__(DownloadQueue)
            return cls.instance
        return cls.instance

    def load(self) -> None:
        self.request_history.clear()
        if os.path.exists(config.request_history_file):
            with open(config.request_history_file, "rb") as file:
                self.request_history = pickle.load(file)

    def save(self) -> None:
        if not os.path.exists(config.history_base_file):
            os.mkdir(config.history_base_file)
        with open(config.request_history_file, "wb") as file:
            pickle.dump(self.request_history, file)

    def track(self, timestamp: datetime) -> None:
        self.request_history.append(timestamp)

    def update(self) -> None:
        differential = datetime.datetime.now() + datetime.timedelta(minutes=-1)
        i = 0
        while True:
            if i == len(self.request_history):
                break
            elif self.request_history[i] < differential:
                self.request_history.pop(i)
            else:
                i += 1

    def wait(self) -> None:
        while True:
            self.update()
            if len(self.request_history) < config.rate:
                break
            time.sleep(1)


queue = DownloadQueue()
