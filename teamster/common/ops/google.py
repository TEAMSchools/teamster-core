import json

from dagster import op


@op(required_resource_keys={"secret_manager"}, config_schema={"secret_name": str})
def access_latest_secret(context):
    secret_version_path = context.resources.secret_manager.secret_path(
        project_id=context.resources.secret_manager.transport._credentials.project_id,
        secret_name=context.op_config["secret_name"],
        secret_version="latest",
    )

    access_response = context.resources.secret_manager.access_secret_version(
        name=secret_version_path
    )

    return json.loads(access_response.payload.data.decode("UTF-8"))


@op(
    required_resource_keys={"secret_manager"},
    config_schema={"secret_name": str, "secret_value": str},
)
def update_latest_secret(context):
    secret_path = context.resources.secret_manager.secret_path(
        project_id=context.resources.secret_manager.transport._credentials.project_id,
        secret_name=context.op_config["secret_name"],
    )
    secret_payload = json.dumps(context.op_config["secret_value"]).encode("UTF-8")

    existing_versions = context.resources.secret_manager.list_secret_versions(
        parent=secret_path
    )

    add_response = context.resources.secret_manager.add_secret_version(
        parent=secret_path, payload={"data": secret_payload}
    )

    # disable previous versions
    for v in existing_versions:
        disable_response = context.resources.secret_manager.disable_secret_version(
            name=v.name
        )


# create secret
@op(required_resource_keys={"secret_manager"}, config_schema={"secret_name": str})
def create_new_secret(context):
    secret_project_path = context.resources.secret_manager.common_project_path(
        project_id=context.resources.secret_manager.transport._credentials.project_id,
    )

    create_response = context.resources.secret_manager.create_secret(
        request={
            "parent": secret_project_path,
            "secret_id": context.op_config["secret_name"],
            "secret": {"replication": {"automatic": {}}},
        }
    )

    # call add secret version?
