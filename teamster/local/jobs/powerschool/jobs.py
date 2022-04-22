from dagster import config_from_files
from teamster.common.graphs.powerschool import run_queries
from teamster.common.resources.google import (
    gcs_file_manager,
    gcs_jsongz_io_manager,
    gcs_resource,
)
from teamster.common.resources.powerschool import powerschool

powerschool_test_job = run_queries.to_job(
    name="powerschool_test_job",
    resource_defs={
        "powerschool": powerschool,
        "gcs": gcs_resource,
        "gcs_io": gcs_jsongz_io_manager,
        "gcs_fm": gcs_file_manager,
    },
    config=config_from_files(
        [
            "./teamster/common/config/powerschool/resource.yaml",
            "./teamster/common/config/google/resource.yaml",
            "./teamster/local/config/powerschool/query-test.yaml",
        ]
    ),
)

__all__ = ["powerschool_test_job"]
