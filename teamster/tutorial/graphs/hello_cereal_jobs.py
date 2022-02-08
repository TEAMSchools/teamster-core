from dagster import job
from tutorial.ops.hello_cereal import (
    display_results,
    download_cereals,
    download_csv,
    find_highest_calorie_cereal,
    find_highest_protein_cereal,
    find_sugariest,
    hello_cereal,
    sort_by_calories,
)


@job
def hello_cereal_job():
    hello_cereal()


@job
def serial():
    find_sugariest(download_cereals())


@job
def diamond():
    cereals = download_cereals()
    display_results(
        most_calories=find_highest_calorie_cereal(cereals),
        most_protein=find_highest_protein_cereal(cereals),
    )


@job
def configurable_job():
    sort_by_calories(download_csv())
