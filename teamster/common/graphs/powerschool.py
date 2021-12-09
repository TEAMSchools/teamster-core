from dagster import graph, configured

from common.ops.powerschool import (
    get_client,
    compose_query_expression,
    get_table,
    query_count,
    query_data,
)


@graph
def extract():
    # instantiate PS client (load config YAML)
    ps = get_client()

    # load query config (YAML)
    year_id = 31
    table_queries = [{"table_name": "schools"}]

    # for tq in table_queries:
    tq = table_queries[0]
    table_name = tq["table_name"]
    q = tq.get("q")
    projection = tq.get("projection")

    # parse params
    if q:
        compose_query_expression_configured = configured(
            compose_query_expression, name="compose_query_expression_configured"
        )({"q": q, "year_id": year_id})
        composed_query = compose_query_expression_configured()
    else:
        composed_query = None

    # if q param, check if destination folder exists
    # if not, generate historical queries

    get_table_configured = configured(get_table, name="get_table_configured")(
        {"table_name": table_name}
    )
    table = get_table_configured(ps)

    query_count_configured = configured(query_count, name="query_count_configured")(
        {"q": composed_query}
    )
    positive_count, zero_count = query_count_configured(table)

    query_data_configured = configured(query_data, name="query_data_configured")(
        {"q": composed_query, "projection": projection}
    )
    data = query_data_configured(positive_count)

    # save to data lake"""
