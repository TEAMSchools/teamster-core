from dagster import config_from_files, file_relative_path

from common.graphs.powerschool import extract
from common.resources.powerschool import powerschool

extract_job = extract.to_job(
    resource_defs={"powerschool": powerschool},
    config=config_from_files(
        [
            file_relative_path(__file__, "../config/powerschool/resource.yaml"),
            # file_relative_path(__file__, "../config/powerschool/test-queries.yaml"),
        ]
    ),
)
