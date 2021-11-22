from dagster import resource
from google.cloud import secretmanager


@resource
def google_secret(init_context, secret_name):
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=secret_name)
    return response.payload.data.decode("UTF-8")
