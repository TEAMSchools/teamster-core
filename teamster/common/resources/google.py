import io
import json
import uuid

from dagster import Field, StringSource, check, io_manager, resource
from dagster.core.storage.file_manager import check_file_like_obj
from dagster.utils.backoff import backoff
from dagster.utils.merger import merge_dicts
from dagster_gcp.gcs.file_manager import GCSFileHandle, GCSFileManager
from dagster_gcp.gcs.io_manager import PickledObjectGCSIOManager
from dagster_gcp.gcs.resources import GCS_CLIENT_CONFIG

from google.api_core.exceptions import Forbidden, TooManyRequests
from google.cloud import storage  # type: ignore


class JSONObjectGCSIOManager(PickledObjectGCSIOManager):
    def __init__(self, bucket, client=None, prefix="dagster"):
        super().__init__(bucket, client, prefix)

    def load_input(self, context):
        key = self._get_path(context.upstream_output)
        context.log.debug(f"Loading GCS object from: {self._uri_for_key(key)}")

        bytes_obj = self.bucket_obj.blob(key).download_as_bytes()
        obj = json.loads(bytes_obj)

        return obj

    def handle_output(self, context, obj):
        key = self._get_path(context)
        context.log.debug(f"Writing GCS object at: {self._uri_for_key(key)}")

        if self._has_object(key):
            context.log.warning(f"Removing existing GCS key: {key}")
            self._rm_object(key)

        json_obj = json.dumps(obj)

        backoff(
            self.bucket_obj.blob(key).upload_from_string,
            args=[json_obj],
            retry_on=(TooManyRequests, Forbidden),
        )


class GCSFileManager(GCSFileManager):
    def __init__(self, client, gcs_bucket, gcs_base_key):
        super().__init__(client, gcs_bucket, gcs_base_key)

    def write_data(self, data, ext=None, **kwargs):
        check.inst_param(data, "data", bytes)
        return self.write(io.BytesIO(data), mode="wb", ext=ext, **kwargs)

    def write(self, file_obj, mode="wb", file_key=str(uuid.uuid4()), ext=None):
        check_file_like_obj(file_obj)

        bucket_obj = self._client.bucket(self._gcs_bucket)
        gcs_key = self.get_full_key(file_key + (("." + ext) if ext is not None else ""))

        bucket_obj.blob(gcs_key).upload_from_file(file_obj)
        return GCSFileHandle(self._gcs_bucket, gcs_key)


@io_manager(
    config_schema={
        "gcs_credentials": Field(StringSource, is_required=False),
        "gcs_bucket": Field(StringSource),
        "gcs_prefix": Field(StringSource, is_required=False, default_value="dagster"),
    },
    required_resource_keys={"gcs"},
)
def gcs_json_io_manager(init_context):
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
    json_io_manager = JSONObjectGCSIOManager(
        init_context.resource_config["gcs_bucket"],
        client,
        init_context.resource_config["gcs_prefix"],
    )
    return json_io_manager


@resource(
    merge_dicts(
        GCS_CLIENT_CONFIG,
        {
            "gcs_credentials": Field(StringSource, is_required=False),
            "gcs_bucket": Field(StringSource),
            "gcs_prefix": Field(
                StringSource, is_required=False, default_value="dagster"
            ),
        },
    )
)
def gcs_file_manager(context):
    """FileManager that provides abstract access to GCS.
    Implements the :py:class:`~dagster.core.storage.file_manager.FileManager` API.
    """
    gcs_client = _gcs_client_from_config(context.resource_config)
    return GCSFileManager(
        client=gcs_client,
        gcs_bucket=context.resource_config["gcs_bucket"],
        gcs_base_key=context.resource_config["gcs_prefix"],
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
