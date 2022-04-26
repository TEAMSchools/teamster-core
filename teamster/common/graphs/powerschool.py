from dagster import graph
from teamster.common.ops.powerschool import compose_queries, query_count, query_data


@graph
def get_query_data(dynamic_query):
    # split DynamicOutput
    # table, query, projection = split_dynamic_output(dynamic_query=dynamic_query)

    # get expected record count, end if 0
    # count, no_data = query_count(table=table, query=query)
    table, query, projection, count, no_count = query_count(dynamic_query=dynamic_query)

    # get data and save to data lake
    data = query_data(  # noqa: F841
        table=table, count=count, query=query, projection=projection
    )

    # merge into db table


@graph
def run_queries():
    # parse queries from run config file (config/powerschool/query-*.yaml)
    dynamic_queries = compose_queries()

    # run sub-graph for each query
    dynamic_queries.map(get_query_data)
