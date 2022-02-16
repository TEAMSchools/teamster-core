from dagster import DynamicOut, DynamicOutput, Out, Output, op

from powerschool import utils


@op(required_resource_keys={"powerschool"})
def get_ps_client(context):
    return context.resources.powerschool


@op(config_schema={"year_id": int, "tables": list}, out={"client": DynamicOut()})
def compose_queries(context, client):
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

            client.table_name = table_name
            client.composed_query = composed_query
            client.projection = projection

            yield DynamicOutput(
                value=client,
                output_name="client",
                mapping_key=f"{table_name}_{i}_{j}",
            )


@op(out={"table": Out()})
def get_table(context, client):
    yield Output(client.get_schema_table(client.table_name), "table")


@op(out={"table": Out(is_required=False), "no_data": Out(is_required=False)})
def query_count(context, table):
    table.query_count = table.count(q=table.client.composed_query)

    if table.query_count > 0:
        yield Output(table, "table")
    else:
        yield Output(None, "no_data")


@op(
    out={
        "data": Out(
            is_required=False,
            io_manager_key="gcs_io",
        ),
        "count_error": Out(is_required=False),
    }
)
def query_data(context, table):
    composed_query = table.client.composed_query
    projection = table.client.projection
    file_key_parts = [table.name, str(composed_query or "")]

    data = table.query(q=composed_query, projection=projection)

    len_data = len(data)
    if len_data < table.query_count:
        updated_count = table.count(q=composed_query)
        if len_data < updated_count:
            yield Output(None, "count_error")  # TODO: raise exception
    else:
        yield Output(value=(data, file_key_parts), output_name="data")


# # check if data exists for specified table
# if not [f for f in file_dir.iterdir()]:
#     # generate historical queries
#     print("\tNo existing data. Generating historical queries...")
#     query_params = utils.generate_historical_queries(
#         current_yearid, selector
#     )
#     query_params.reverse()
