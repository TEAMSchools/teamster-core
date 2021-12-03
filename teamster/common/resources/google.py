from dagster import resource
from google.cloud import secretmanager


@resource(
    config_schema={"google_application_credentials": str},
)
def secret_manager(init_context):
    return secretmanager.SecretManagerServiceClient.from_service_account_json(
        init_context.resource_config["google_application_credentials"]
    )
