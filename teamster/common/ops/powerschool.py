from dagster import DynamicOut, DynamicOutput, Out, Output, op

from powerschool import utils


@op(required_resource_keys={"powerschool"})
def get_ps_client(context):
    return context.resources.powerschool


@op(
    config_schema={"year_id": int, "tables": list},
    out={"query": DynamicOut(dict)},
)
def compose_queries(context):
    year_id = context.op_config["year_id"]
    tables = context.op_config["tables"]

    for i, t in enumerate(tables):
        table_name = t["name"]
        queries = t.get("q", [{}])
        projection = t.get("projection")

        for x, q in enumerate(queries):
            if q:
                selector = q.get("selector")
                value = q.get("value", utils.transform_yearid(year_id, selector))

                constraint_rules = utils.get_constraint_rules(selector, year_id)
                constraint_values = utils.get_constraint_values(
                    selector, value, constraint_rules["step_size"]
                )
                composed_query = utils.get_query_expression(
                    selector, **constraint_values
                )
            else:
                composed_query = None

            query = {
                "table_name": table_name,
                "q": composed_query,
                "projection": projection,
            }

            yield DynamicOutput(
                value=query,
                output_name="query",
                mapping_key=f"{table_name}_{i}_{x}",
            )


@op
def get_table(context, client, query):
    table_name = query["table_name"]

    table = client.get_schema_table(table_name)

    query["table"] = table
    return query


@op(out={"table_query": Out(is_required=False), "no_data": Out(is_required=False)})
def query_count(context, query):
    table = query["table"]
    q = query["q"]

    count = table.count(q=q)

    if count > 0:
        query["count"] = count
        yield Output(query, "table_query")
    else:
        yield Output(None, "no_data")


@op(out={"data": Out(is_required=False), "count_error": Out(is_required=False)})
def query_data(context, query):
    table = query["table"]
    q = query["q"]
    projection = query["projection"]
    count = query["count"]

    data = table.query(q=q, projection=projection)

    len_data = len(data)
    if len_data < count:
        updated_count = table.count(q=q)
        if len_data < updated_count:
            yield Output(None, "count_error")  # TODO: raise exception
    else:
        yield Output(data, "data")


# # check if data exists for specified table
# if not [f for f in file_dir.iterdir()]:
#     # generate historical queries
#     print("\tNo existing data. Generating historical queries...")
#     query_params = utils.generate_historical_queries(
#         current_yearid, selector
#     )
#     query_params.reverse()
