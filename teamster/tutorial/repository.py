from dagster import repository

# from teamster.graphs.say_hello import say_hello_job
from tutorial.graphs.hello_cereal_jobs import (
    configurable_job,
    diamond,
    hello_cereal_job,
    serial,
)

# from teamster.schedules.my_hourly_schedule import my_hourly_schedule
# from teamster.sensors.my_sensor import my_sensor


@repository
def tutorial():
    """
    The repository definition for this teamster Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [hello_cereal_job, serial, diamond, configurable_job]
    schedules = []
    sensors = []

    return jobs + schedules + sensors
