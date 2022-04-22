from dagster import repository

from teamster.local.jobs import powerschool_test_job


@repository
def dev():
    jobs = [powerschool_test_job]
    schedules = []
    sensors = []

    return jobs + schedules + sensors
