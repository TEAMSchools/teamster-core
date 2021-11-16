from dagster import repository

from teamster.graphs.say_hello import say_hello_job
from teamster.schedules.my_hourly_schedule import my_hourly_schedule
from teamster.sensors.my_sensor import my_sensor


@repository
def teamster():
    """
    The repository definition for this teamster Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [say_hello_job]
    schedules = [my_hourly_schedule]
    sensors = [my_sensor]

    return jobs + schedules + sensors
