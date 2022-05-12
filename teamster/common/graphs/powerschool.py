from dagster import graph
from teamster.common.ops.powerschool import compose_queries, get_count, get_data


@graph
def query_data(dynamic_query):
    # split DynamicOutput and get record count, end if 0
    table, query, projection, count, n_pages, no_count = get_count(
        dynamic_query=dynamic_query
    )

    # get data and save to data lake
    get_data(
        table=table, query=query, projection=projection, count=count, n_pages=n_pages
    )


@graph
def run_queries():
    # parse queries from run config file (config/powerschool/query-*.yaml)
    dynamic_queries = compose_queries()

    # run sub-graph for each query
    dynamic_queries.map(query_data)
