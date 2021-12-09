from dagster import repository

from kippnewark.jobs.powerschool import extract_job

@repository
def kippnewark():
    jobs = [extract_job]
    schedules = []
    sensors = []

    return jobs + schedules + sensors
