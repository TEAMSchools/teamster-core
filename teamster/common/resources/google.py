import gzip
import json
import uuid

from dagster import _check as check
from dagster.config import Field
from dagster.config.source import StringSource
from dagster.core.definitions import resource
from dagster.core.events import DagsterEventType
from dagster.core.storage.io_manager import io_manager
from dagster.utils.backoff import backoff
from dagster.utils.merger import merge_dicts
from dagster_gcp.gcs.file_manager import GCSFileHandle, GCSFileManager
from dagster_gcp.gcs.io_manager import PickledObjectGCSIOManager
from dagster_gcp.gcs.resources import GCS_CLIENT_CONFIG, _gcs_client_from_config
from google.api_core.exceptions import Forbidden, TooManyRequests


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


class GCSFileManager(GCSFileManager):
    def __init__(self, client, gcs_bucket, gcs_base_key):
        super().__init__(client, gcs_bucket, gcs_base_key)
        self.bucket_obj = self._client.bucket(self._gcs_bucket)

    def _rm_object(self, key):
        check.str_param(key, "key")
        check.param_invariant(len(key) > 0, "key")

        if self.bucket_obj.blob(key).exists():
            self.bucket_obj.blob(key).delete()

    def _has_object(self, key):
        check.str_param(key, "key")
        check.param_invariant(len(key) > 0, "key")

        key = self.get_full_key(key)
        blobs = self._client.list_blobs(self._gcs_bucket, prefix=key)

        return len(list(blobs)) > 0

    def _uri_for_key(self, key):
        check.str_param(key, "key")
        return f"gs://{self._gcs_bucket}/{key}"

    def upload_from_string(self, context, obj, ext=None, file_key=None):
        key = self.get_full_key(
            file_key or (str(uuid.uuid4()) + (("." + ext) if ext is not None else ""))
        )

        context.log.debug(f"Writing GCS object at: {self._uri_for_key(key=key)}")

        if self._has_object(key):
            context.log.warning(f"Removing existing GCS key: {key}")
            self._rm_object(key)

        backoff(
            self.bucket_obj.blob(key).upload_from_string,
            args=[obj],
            retry_on=(TooManyRequests, Forbidden),
        )

        return GCSFileHandle(self._gcs_bucket, key)


@resource(
    merge_dicts(
        GCS_CLIENT_CONFIG,
        {
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
