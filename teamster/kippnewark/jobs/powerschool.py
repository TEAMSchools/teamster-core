import pathlib

from dagster import config_from_files

from teamster.common.graphs.powerschool import powerschool_test_extract
from teamster.common.resources.google import gcs_jsongz_io_manager, gcs_resource
from teamster.common.resources.powerschool import powerschool

cwd = pathlib.Path.cwd()
file_path = pathlib.Path(__file__).absolute()
repo_name = [item for item in file_path.parts if item not in cwd.parts][0]

powerschool_test_extract_job = powerschool_test_extract.to_job(
    resource_defs={
        "powerschool": powerschool,
        "gcs": gcs_resource,
        "gcs_io": gcs_jsongz_io_manager,
    },
    config=config_from_files(
        [
            f"./teamster/config/{repo_name}/powerschool/resource.yaml",
            f"./teamster/config/{repo_name}/powerschool/query-test.yaml",
            f"./teamster/config/{repo_name}/google/resource.yaml",
        ]
    ),
)


"""
job1 = configurable_graph.to_job(
    resource_defs={
        "foo": bar1,
        "spam": eggs1,
    },
    config=config_from_files(
        [
            f"./config/resource.yaml",
            f"./config/queries-1.yaml",
        ]
    ),
)

job2 = configurable_graph.to_job(
    resource_defs={
        "foo": bar2,
        "spam": eggs2,
    },
    config=config_from_files(
        [
            f"./config/resource.yaml",
            f"./config/queries-2.yaml",
        ]
    ),
)
"""
