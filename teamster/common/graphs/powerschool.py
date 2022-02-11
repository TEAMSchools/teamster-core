from dagster import graph

from ..ops.powerschool import (
    compose_queries,
    get_ps_client,
    get_table,
    query_count,
    query_data,
)


@graph
def powerschool_test_extract():
    # instantiate PS client w/ auth (config/powerschool/resource.yaml)
    ps = get_ps_client()

    # parse queries from run config file (config/powerschool/query-*.yaml)
    # DynamicOutput
    queries = compose_queries()

    # for each query
    # instantiate table object
    tables = queries.map(lambda q: get_table(client=ps, query=q))

    # get expected record count, end if == 0
    count, no_data = tables.map(query_count)

    # get data and save to data lake
    data, count_error = count.map(query_data)

    # if q param, check if destination folder exists
    # if not, generate backfill queries

    # merge into db table
