import os
import signal
from contextlib import contextmanager
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dagster.core.storage.pipeline_run import RunsFilter, DagsterRunStatus

LOCAL_TIMEZONE = ZoneInfo(os.getenv("LOCAL_TIMEZONE"))
TODAY = datetime.now(tz=LOCAL_TIMEZONE)
YESTERDAY = TODAY - timedelta(days=1)


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


def get_last_successful_schedule_run(context):
    runs = context.instance.get_runs(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.SUCCESS],
            job_name=context.job_name,
            tags={
                "dagster/schedule_name": context.get_tag(key="dagster/schedule_name")
            },
        ),
        limit=1,
    )

    return runs[0] if runs else None
