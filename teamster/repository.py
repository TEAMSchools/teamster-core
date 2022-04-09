import requests
from dagster import job, op, repository


@op
def get_ip(context):
    resp = requests.get("https://api.ipify.org")
    context.log.debug(resp.text)


@job
def get_ip_job():
    get_ip()


@repository
def my_repository():
    return [get_ip_job]
