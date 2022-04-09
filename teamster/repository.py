import requests
from dagster import job, op, repository


@op
def get_ip(context):
    resp = requests.get("https://www.google.com", stream=True)
    ip = resp.raw._connection.sock.getsockname()
    context.log.debug(ip)


@job
def get_ip_job():
    get_ip()


@repository
def my_repository():
    return [get_ip_job]
