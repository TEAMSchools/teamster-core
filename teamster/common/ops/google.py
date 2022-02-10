import json

from dagster import op


@op(required_resource_keys={"gcs"})
def save_to_gcs(context, data):
    data_bytes = json.dumps(data).encode("utf-8")
    context.resources.gcs.write_data(data_bytes, ext="json.gz")
