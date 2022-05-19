import os
import signal
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from dagster.core.storage.pipeline_run import RunsFilter, DagsterRunStatus

LOCAL_TIME_ZONE = ZoneInfo(os.getenv("LOCAL_TIME_ZONE"))
TODAY = datetime.now(tz=LOCAL_TIME_ZONE)
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


def get_last_schedule_run(context):
    runs = context.instance.get_run_records(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.SUCCESS],
            job_name=context.job_name,
            tags={
                "dagster/schedule_name": context.get_tag(key="dagster/schedule_name")
            },
        ),
        limit=1,
    )

    last_run = runs[0] if runs else None
    if last_run:
        return last_run.create_timestamp.astimezone(tz=LOCAL_TIME_ZONE)
    else:
        # use UNIX Epoch if schedule never ran
        return datetime(1970, 1, 1, tzinfo=timezone.utc).astimezone(tz=LOCAL_TIME_ZONE)
