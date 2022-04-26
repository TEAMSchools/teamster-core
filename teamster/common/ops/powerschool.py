from dagster import op
from dagster import Any, Dict, Int, List, Nothing, Optional, String, Tuple
from dagster import In, Out, DynamicOut, Output, DynamicOutput
from dagster import RetryRequested, RetryPolicy, Backoff
from powerschool.utils import (
    generate_historical_queries,
    get_constraint_rules,
    get_constraint_values,
    get_query_expression,
    transform_year_id,
)
from requests.exceptions import ConnectionError

from teamster.common.config.powerschool import PS_QUERY_CONFIG
from teamster.common.utils import time_limit


@op(
    config_schema=PS_QUERY_CONFIG,
    out={"dynamic_query": DynamicOut(dagster_type=Tuple)},
    required_resource_keys={"powerschool"},
)
def compose_queries(context):
    tables = context.op_config["tables"]
    year_id = context.op_config.get("year_id")

    for tbl in tables:
        table = context.resources.powerschool.get_schema_table(tbl["name"])
        queries = tbl.get("queries", {})

        filtered_queries = [fq for fq in queries if fq.get("q")]
        if filtered_queries:
            for i, fq in enumerate(filtered_queries):
                fq_projection = fq.get("projection")
                q = fq.get("q")

                if isinstance(q, str):
                    yield DynamicOutput(
                        value=(table, q, fq_projection),
                        output_name="dynamic_query",
                        mapping_key=f"{table.name}_q_{i}",
                    )
                else:
                    selector = q.get("selector")
                    value = q.get("value", transform_year_id(year_id, selector))

                    if value == "resync":
                        context.log.info(
                            f"Generating historical queries for {table.name}"
                        )

                        hq_projection = next(
                            iter(
                                [
                                    pj["projection"]
                                    for pj in queries
                                    if pj.get("projection")
                                ]
                            ),
                            None,
                        )

                        max_value = q.get("max_value")
                        # set max historical value for "id" queries to 1.5x count
                        if not max_value and selector[-2:] == "id":
                            max_value = int(table.count() * 1.5)
                        context.log.debug(f"max_value: {max_value}")

                        hist_query_exprs = generate_historical_queries(
                            year_id, selector, max_value=max_value
                        )
                        hist_query_exprs.reverse()

                        for j, hq in enumerate(hist_query_exprs):
                            # context.log.debug(hq)
                            # hq_count = table.count(q=hq)
                            # if hq_count > 0:
                            yield DynamicOutput(
                                value=(table, hq, hq_projection),
                                output_name="dynamic_query",
                                mapping_key=f"{table.name}_hq_{j}",
                            )
                    else:
                        constraint_rules = get_constraint_rules(selector, year_id)
                        constraint_values = get_constraint_values(
                            selector, value, constraint_rules["step_size"]
                        )
                        composed_query = get_query_expression(
                            selector, **constraint_values
                        )

                        yield DynamicOutput(
                            value=(table, composed_query, fq_projection),
                            output_name="dynamic_query",
                            mapping_key=f"{table.name}_q_{i}",
                        )
        else:
            # use 1st listed projection if present
            projection = next(
                iter([pj["projection"] for pj in queries if pj.get("projection")]), None
            )

            yield DynamicOutput(
                value=(table, None, projection),
                output_name="dynamic_query",
                mapping_key=table.name,
            )


# @op(
#     ins={"dynamic_query": In(dagster_type=Tuple)},
#     out={
#         "table": Out(dagster_type=Any),
#         "query": Out(dagster_type=Optional[String]),
#         "projection": Out(dagster_type=Optional[String]),
#     },
# )
# def split_dynamic_output(dynamic_query):
#     table, query, projection = dynamic_query

#     yield Output(value=table, output_name="table")
#     yield Output(value=query, output_name="query")
#     yield Output(value=projection, output_name="projection")


@op(
    # ins={"table": In(dagster_type=Any), "query": In(dagster_type=Optional[String])},
    ins={"dynamic_query": In(dagster_type=Tuple)},
    out={
        "table": Out(dagster_type=Any),
        "query": Out(dagster_type=Optional[String]),
        "projection": Out(dagster_type=Optional[String]),
        "count": Out(dagster_type=Int, is_required=False),
        "no_count": Out(dagster_type=Nothing, is_required=False),
    },
    retry_policy=RetryPolicy(max_retries=1, delay=1, backoff=Backoff.EXPONENTIAL),
)
# def query_count(context, table, query):
def query_count(context, dynamic_query):
    table, query, projection = dynamic_query

    context.log.debug(f"{table.name}\n{query}")

    try:
        count = table.count(q=query)
    except ConnectionError as e:
        raise RetryRequested() from e

    context.log.info(f"Found {count} records")

    if count > 0:
        yield Output(value=table, output_name="table")
        yield Output(value=query, output_name="query")
        yield Output(value=projection, output_name="projection")
        yield Output(value=count, output_name="count")
    else:
        yield Output(value=None, output_name="no_count")


@op(
    ins={
        "table": In(dagster_type=Any),
        "query": In(dagster_type=Optional[String]),
        "projection": In(dagster_type=Optional[String]),
        "count": In(dagster_type=Int),
    },
    out={
        "data": Out(dagster_type=List[Dict], is_required=False, io_manager_key="gcs_io")
    },
    retry_policy=RetryPolicy(max_retries=1, delay=60, backoff=Backoff.EXPONENTIAL),
)
def query_data(context, table, query, projection, count):
    file_key_parts = [table.name, str(query or "")]
    file_stem = "_".join(filter(None, file_key_parts))
    file_ext = "json.gz"
    file_key = f"{file_key_parts[0]}/{file_stem}.{file_ext}"

    context.log.debug(f"{table.name}\n{query}\n{projection}")

    try:
        with time_limit(7200):
            data = table.query(q=query, projection=projection)
    except TimeoutError as e:
        raise RetryRequested() from e
    except ConnectionError as e:
        raise RetryRequested() from e

    len_data = len(data)
    if len_data < count:
        updated_count = table.count(q=query)
        if len_data < updated_count:
            raise RetryRequested(
                f"Data received is less than count: {len_data} < {count}"
            )
    else:
        yield Output(value=data, output_name="data", metadata={"file_key": file_key})
