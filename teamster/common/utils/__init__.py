import os
import signal
from contextlib import contextmanager
from datetime import datetime, timedelta
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


LOCAL_TIMEZONE = ZoneInfo(os.getenv("LOCAL_TIMEZONE"))
TODAY = datetime.now(tz=LOCAL_TIMEZONE)
YESTERDAY = TODAY - timedelta(days=1)
