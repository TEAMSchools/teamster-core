from dagster import graph, configured

from common.ops.powerschool import (
    get_client,
    compose_query_expression,
    get_table,
    query_count,
    query_data,
    query_spooler,
)


@graph
def powerschool_test_extract():
    # instantiate PS client w/ auth (config/powerschool/resource.yaml)
    ps = get_client()

    # parse queries from run config file (config/powerschool/query-*.yaml)
    # DynamicOutput
    query = query_spooler(client=ps)

    # for each query
    # instantiate table object
    table = query.map(get_table)

    # compose query expression from table config
    composed_query = table.map(compose_query_expression)

    # if q param, check if destination folder exists
    # if not, generate historical queries

    # get expected record count, end if == 0
    count, no_data = composed_query.map(query_count)

    # get data
    data, count_error = count.map(query_data)

    # save to data lake

    # merge into db table
