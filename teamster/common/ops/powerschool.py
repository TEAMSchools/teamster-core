from dagster import DynamicOut, DynamicOutput, Out, Output, op

from powerschool import utils


@op(required_resource_keys={"powerschool"})
def get_client(context):
    return context.resources.powerschool


@op(
    config_schema={"year_id": int, "tables": list},
    out={"table_query": DynamicOut(dict)},
)
def query_spooler(context, client):
    year_id = context.op_config["year_id"]
    tables = context.op_config["tables"]

    for i, t in enumerate(tables):
        table_name = t["name"]
        queries = t.get("q", [{}])
        projection = t.get("projection")

        for x, q in enumerate(queries):
            table_query = {
                "client": client,
                "year_id": year_id,
                "table_name": table_name,
                "q": q,
                "projection": projection,
            }

            yield DynamicOutput(
                value=table_query,
                output_name="table_query",
                mapping_key=f"{table_name}_{i}_{x}",
            )


@op
def compose_query_expression(context, table_query):
    year_id = table_query["year_id"]
    q = table_query["q"]

    if not q:
        query_expression = None
    else:
        selector = q.get("selector")
        value = q.get("value", utils.transform_yearid(year_id, selector))

        constraint_rules = utils.get_constraint_rules(selector, year_id)
        constraint_values = utils.get_constraint_values(
            selector, value, constraint_rules["step_size"]
        )
        query_expression = utils.get_query_expression(selector, **constraint_values)

    table_query["query_expression"] = query_expression

    return table_query


# # check if data exists for specified table
# if not [f for f in file_dir.iterdir()]:
#     # generate historical queries
#     print("\tNo existing data. Generating historical queries...")
#     query_params = utils.generate_historical_queries(
#         current_yearid, selector
#     )
#     query_params.reverse()


@op
def get_table(context, table_query):
    client = table_query["client"]
    table_name = table_query["table_name"]

    table = client.get_schema_table(table_name)
    table_query["table"] = table

    return table_query


@op(out={"table_query": Out(is_required=False), "no_data": Out(is_required=False)})
def query_count(context, table_query):
    table = table_query["table"]
    q = table_query["query_expression"]

    count = table.count(q=q)
    if count > 0:
        table_query["count"] = count
        yield Output(table_query, "table_query")
    else:
        yield Output(None, "no_data")


@op(out={"data": Out(is_required=False), "count_error": Out(is_required=False)})
def query_data(context, table_query):
    table = table_query["table"]
    q = table_query["query_expression"]
    projection = table_query["projection"]
    count = table_query["count"]

    data = table.query(q=q, projection=projection)

    len_data = len(data)
    if len_data < count:
        updated_count = table.count(q=q)
        if len_data < updated_count:
            yield Output(None, "count_error")  # TODO: raise exception
    else:
        yield Output(data, "data")
