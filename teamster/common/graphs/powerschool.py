from dagster import graph

from teamster.common.ops.powerschool import (
    compose_queries,
    get_table,
    get_ps_client,
    query_count,
    query_data,
    split_dynamic_output,
)


@graph
def powerschool_extract_graph(client, dynamic_query):
    # split DynamicOutput
    table_name, query, projection = split_dynamic_output(dynamic_output=dynamic_query)

    # instantiate table object
    table = get_table(client=client, table_name=table_name)

    # get expected record count, end if 0
    count, no_data = query_count(table=table, query=query)

    # get data and save to data lake
    data, count_error = query_data(
        table=table, query=query, projection=projection, count=count
    )

    # if q param, check if destination folder exists
    # if not, generate backfill queries

    # merge into db table


@graph
def powerschool_extract():
    # instantiate PS client w/ auth (config/powerschool/resource.yaml)
    ps = get_ps_client()

    # parse queries from run config file (config/powerschool/query-*.yaml)
    dynamic_queries = compose_queries()

    # run sub-graph for each query
    dynamic_queries.map(
        lambda dq: powerschool_extract_graph(client=ps, dynamic_query=dq)
    )
