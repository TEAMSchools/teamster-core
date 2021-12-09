from datetime import datetime

from dagster import Any, DynamicOut, DynamicOutput, Field, Noneable, Out, Output, op, config_from_files

from powerschool import utils


@op(required_resource_keys={"powerschool"})
def get_client(context):
    return context.resources.powerschool


@op(
    config_schema={"year_id": int, "queries": dict},
    out=DynamicOut(dict),
)
def queries_to_execute(context):
    queries = context.op_config["queries"]
    for q in queries:
        yield DynamicOutput(
            value=q,
            mapping_key=q["table_name"],
        )


@op(config_schema={"q": dict, "year_id": int})
def compose_query_expression(context):
    year_id = context.op_config["year_id"]
    selector = context.op_config["q"]["selector"]
    value = context.op_config["q"].get("value")

    if not value:
        value = [utils.transform_yearid(year_id, selector)]
    elif value == "yesterday":
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        query_expression = f"{selector}=ge={yesterday.isoformat()}"
    else:
        constraint_rules = utils.get_constraint_rules(selector, year_id)
        constraint_values = utils.get_constraint_values(
            selector, value, constraint_rules["step_size"]
        )
        query_expression = utils.get_query_expression(selector, **constraint_values)

    return query_expression


# # check if data exists for specified table
# if not [f for f in file_dir.iterdir()]:
#     # generate historical queries
#     print("\tNo existing data. Generating historical queries...")
#     query_params = utils.generate_historical_queries(
#         current_yearid, selector
#     )
#     query_params.reverse()


@op(config_schema={"table_name": str})
def get_table(context, client):
    return client.get_schema_table(context.op_config["table_name"])


@op(
    config_schema={"q": Field(str, is_required=False)},
    out={
        "positive_count": Out(is_required=False),
        "zero_count": Out(is_required=False),
    },
)
def query_count(context, table):
    count = table.count(q=context.op_config["q"])
    if count > 0:
        yield Output(table, "positive_count")
    else:
        yield Output(None, "zero_count")


@op(
    config_schema={
        "q": Field(str, is_required=False),
        "projection": Field(str, is_required=False),
    }
)
def query_data(context, table):
    data = table.query(
        q=context.op_config["q"], projection=context.op_config["projection"]
    )
    return data
