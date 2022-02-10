import pathlib

from dagster import config_from_files

from teamster.common.graphs.powerschool import powerschool_test_extract
from teamster.common.resources.powerschool import powerschool
from teamster.common.resources.google import gcs_file_manager

cwd = pathlib.Path.cwd()
file_path = pathlib.Path(__file__).absolute()
repo_name = [item for item in file_path.parts if item not in cwd.parts][0]

powerschool_test_extract_job = powerschool_test_extract.to_job(
    resource_defs={"powerschool": powerschool, "gcs": gcs_file_manager},
    config=config_from_files(
        [
            f"./teamster/config/{repo_name}/powerschool/resource.yaml",
            f"./teamster/config/{repo_name}/powerschool/query-test.yaml",
            f"./teamster/config/{repo_name}/google/resource.yaml",
        ]
    ),
)
