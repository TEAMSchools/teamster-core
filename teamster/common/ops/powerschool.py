from dagster import DynamicOut, DynamicOutput, Out, Output, op
from powerschool import utils


@op(required_resource_keys={"powerschool"})
def get_ps_client(context):
    return context.resources.powerschool


@op(
    config_schema={"year_id": int, "tables": list},
    out={"query": DynamicOut()},
    required_resource_keys={"gcs_file_manager"},
)
def compose_queries(context):
    year_id = context.op_config["year_id"]
    tables = context.op_config["tables"]

    for i, tbl in enumerate(tables):
        table_name = tbl["name"]
        queries = tbl.get("q", [{}])
        projection = tbl.get("projection")

        for j, q in enumerate(queries):
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

            yield DynamicOutput(
                value=(table_name, composed_query, projection),
                output_name="query",
                mapping_key=f"{table_name}_{i}_{j}",
            )


@op(out={"table_name": Out(), "query": Out(), "projection": Out()})
def split_dynamic_output(context, dynamic_output):
    table_name, query, projection = dynamic_output

    yield Output(table_name, "table_name")
    yield Output(query, "query")
    yield Output(projection, "projection")


@op(out={"table": Out()})
def get_table(context, client, table_name):
    yield Output(client.get_schema_table(table_name), "table")


@op(out={"count": Out(is_required=False), "no_data": Out(is_required=False)})
def query_count(context, table, query):
    count = table.count(q=query)

    if count > 0:
        yield Output(count, "count")
    else:
        yield Output(None, "no_data")


@op(
    out={
        "data": Out(is_required=False, io_manager_key="gcs_io"),
        "count_error": Out(is_required=False),
    }
)
def query_data(context, table, query, projection, count):
    file_key_parts = [table.name, str(query or "")]
    file_stem = "_".join(filter(None, file_key_parts))
    file_ext = "json.gz"
    file_key = f"{file_key_parts[0]}/{file_stem}.{file_ext}"

    data = table.query(q=query, projection=projection)

    len_data = len(data)
    if len_data < count:
        updated_count = table.count(q=query)
        if len_data < updated_count:
            yield Output(value=None, output_name="count_error")  # TODO: raise exception
    else:
        yield Output(value=data, output_name="data", metadata={"file_key": file_key})


# # check if data exists for specified table
# if not [f for f in file_dir.iterdir()]:
#     # generate historical queries
#     print("\tNo existing data. Generating historical queries...")
#     query_params = utils.generate_historical_queries(
#         current_yearid, selector
#     )
#     query_params.reverse()
