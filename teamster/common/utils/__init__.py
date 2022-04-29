import os
import signal
from contextlib import contextmanager
from datetime import datetime
from zoneinfo import ZoneInfo


@contextmanager
def time_limit(seconds):
    def signal_handler(signum, frame):
        raise TimeoutError("Timed out after {seconds}")

    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)


TODAY = datetime.now(ZoneInfo(os.getenv("LOCAL_TIMEZONE")))
