from dagster import repository

from .jobs.powerschool import powerschool_test_extract_job


@repository
def kippnewark():
    jobs = [powerschool_test_extract_job]
    schedules = []
    sensors = []

    return jobs + schedules + sensors
