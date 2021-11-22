from dagster import graph

# from teamster.ops.hello import hello
from common.ops.powerschool import hello
from common.resources.google import google_secret

@graph
def say_hello():
    """
    A graph definition. This example graph has a single op.

    For more hints on writing Dagster graphs, see our documentation overview on Graphs:
    https://docs.dagster.io/concepts/ops_graphs/graphs
    """
    hello()

say_hello_job = say_hello.to_job(resource_defs={ "google_secret": google_secret})
