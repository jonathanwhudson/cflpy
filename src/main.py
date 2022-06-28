import threading
import time
import signal

from datetime import timedelta

MINUTE_IN_SECONDS = 60
REST_POLL_SECONDS = 1 * MINUTE_IN_SECONDS
WAIT_TIME_SECONDS = 1


class ProgramKilled(Exception):
    pass


def foo():
    print(time.ctime())


def signal_handler(signum, frame):
    raise ProgramKilled


class Job(threading.Thread):
    def __init__(self, interval, execute, *args, **kwargs):
        threading.Thread.__init__(self)
        self.daemon = False
        self.stopped = threading.Event()
        self.interval = interval
        self.execute = execute
        self.args = args
        self.kwargs = kwargs

    def stop(self):
        self.stopped.set()
        self.join()

    def run(self):
        while not self.stopped.wait(self.interval.total_seconds()):
            self.execute(*self.args, **self.kwargs)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    job = Job(interval=timedelta(seconds=REST_POLL_SECONDS), execute=foo)
    job.start()
    while True:
        try:
            time.sleep(WAIT_TIME_SECONDS)
        except ProgramKilled:
            print("Program killed: running cleanup code")
            job.stop()
            break
