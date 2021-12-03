from dagster import graph

from common.ops.powerschool import hello
from common.resources.google import google_secret_manager


@graph
def say_hello():
    """
    A graph definition. This example graph has a single op.

    For more hints on writing Dagster graphs, see our documentation overview on Graphs:
    https://docs.dagster.io/concepts/ops_graphs/graphs
    """
    hello()


say_hello_job = say_hello.to_job(
    resource_defs={
        "google_secret_manager": google_secret_manager.configured(
            {
                "google_application_credentials": "/home/cbini/.config/gcloud/teamster-332318-2df6a03d0e10.json"
            }
        ),
    },
    config={
        "ops": {
            "hello": {"config": {"secret_name": "projects/teamster-332318/secrets/spam/versions/latest"}},
        }
    },
)
