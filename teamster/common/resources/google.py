import gzip
import json

from dagster import Field, StringSource, io_manager, resource, DagsterEventType
from dagster.utils.backoff import backoff
from dagster_gcp.gcs.io_manager import PickledObjectGCSIOManager
from dagster_gcp.gcs.resources import GCS_CLIENT_CONFIG

from google.api_core.exceptions import Forbidden, TooManyRequests
from google.cloud import storage  # type: ignore

GCS_CLIENT_CONFIG["gcs_credentials"] = Field(StringSource, is_required=False)


class JsonGzObjectGCSIOManager(PickledObjectGCSIOManager):
    def __init__(self, bucket, client=None, prefix="dagster"):
        super().__init__(bucket, client, prefix)

    def _get_file_key(self, context):
        all_output_logs = context.step_context.instance.all_logs(
            context.run_id, of_type=DagsterEventType.STEP_OUTPUT
        )
        step_output_log = [
            log for log in all_output_logs if log.step_key == context.step_key
        ][0]
        metadata = step_output_log.dagster_event.event_specific_data.metadata_entries

        file_key_entry = next(
            iter([e for e in metadata if e.label == "file_key"]), None
        )

        if file_key_entry:
            return file_key_entry.value.text
        else:
            return None

    def _get_path(self, context):
        if context.file_key:
            return "/".join([self.prefix, context.file_key])
        else:
            parts = context.get_output_identifier(context.step_context.instance)
            run_id = parts[0]
            output_parts = parts[1:]
            return "/".join([self.prefix, "storage", run_id, "files", *output_parts])

    def load_input(self, context):
        context.upstream_output.file_key = self._get_file_key(context.upstream_output)

        key = self._get_path(context.upstream_output)
        context.log.debug(f"Loading GCS object from: {self._uri_for_key(key)}")

        bytes_obj = self.bucket_obj.blob(key).download_as_bytes()
        obj = json.loads(gzip.decompress(bytes_obj))

        return obj

    def handle_output(self, context, obj):
        context.file_key = self._get_file_key(context)

        key = self._get_path(context)

        context.log.debug(f"Writing GCS object at: {self._uri_for_key(key)}")

        if self._has_object(key):
            context.log.warning(f"Removing existing GCS key: {key}")
            self._rm_object(key)

        jsongz_obj = gzip.compress(json.dumps(obj).encode("utf-8"))

        backoff(
            self.bucket_obj.blob(key).upload_from_string,
            args=[jsongz_obj],
            retry_on=(TooManyRequests, Forbidden),
        )


def _gcs_client_from_config(config):
    """
    Args:
        config: A configuration containing the fields in GCS_CLIENT_CONFIG.
    Returns: A GCS client.
    """
    project = config.get("project", None)
    credentials = config.get("gcs_credentials", None)

    if credentials:
        return storage.Client.from_service_account_json(
            json_credentials_path=credentials
        )
    else:
        return storage.client.Client(project=project)


@resource(
    GCS_CLIENT_CONFIG,
    description="This resource provides a GCS client",
)
def gcs_resource(init_context):
    return _gcs_client_from_config(init_context.resource_config)


@io_manager(
    config_schema={
        "gcs_bucket": Field(StringSource),
        "gcs_prefix": Field(StringSource, is_required=False, default_value="dagster"),
    },
    required_resource_keys={"gcs"},
)
def gcs_jsongz_io_manager(init_context):
    """
    Persistent IO manager using GCS for storage.
    Serializes objects via JSON. Suitable for objects storage for distributed
    executors, so long as each execution node has network connectivity and credentials
    for GCS and the backing bucket. Attach this resource definition to your job to make
    it available to your ops.
    .. code-block:: python
        @job(resource_defs={
            'io_manager': gcs_pickle_io_manager, 'gcs': gcs_resource, ...
        })
        def my_job():
            my_op()
    You may configure this storage as follows:
    .. code-block:: YAML
        resources:
            io_manager:
                config:
                    gcs_bucket: my-cool-bucket
                    gcs_prefix: good/prefix-for-files-
    """
    client = init_context.resources.gcs
    json_io_manager = JsonGzObjectGCSIOManager(
        init_context.resource_config["gcs_bucket"],
        client,
        init_context.resource_config["gcs_prefix"],
    )
    return json_io_manager
