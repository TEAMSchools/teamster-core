from dagster import op
from dagster import Any, Dict, Int, List, Nothing, Optional, String, Tuple
from dagster import Field, In, Out, DynamicOut, Output, DynamicOutput
from dagster import RetryRequested, RetryPolicy, Backoff
from powerschool.utils import (
    generate_historical_queries,
    get_constraint_rules,
    get_constraint_values,
    get_query_expression,
    transform_year_id,
)
from requests.exceptions import ConnectionError

from teamster.common.config.powerschool import COMPOSE_QUERIES_CONFIG
from teamster.common.utils import TODAY, time_limit


@op(
    config_schema=COMPOSE_QUERIES_CONFIG,
    out={"dynamic_query": DynamicOut(dagster_type=Tuple)},
    required_resource_keys={"powerschool"},
)
def compose_queries(context):
    tables = context.op_config["tables"]
    year_id = context.op_config.get("year_id")

    for tbl in tables:
        table = context.resources.powerschool.get_schema_table(tbl["name"])
        queries = tbl.get("queries", {})
        projection = tbl.get("projection")

        filtered_queries = [fq for fq in queries if fq.get("q")]
        if filtered_queries:
            for i, fq in enumerate(filtered_queries):
                fq_projection = fq.get("projection", projection)
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

                    if value == "today":
                        value = TODAY.date().isoformat()

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
                        elif not max_value:
                            max_value = transform_year_id(year_id, selector)
                        context.log.debug(f"max_value: {max_value}")

                        # get step and stoppage critera for constraint type
                        constraint_rules = get_constraint_rules(
                            selector, year_id=year_id, is_historical=True
                        )

                        hq_expressions = generate_historical_queries(
                            selector=selector,
                            start_value=max_value,
                            stop_value=constraint_rules["stop_value"],
                            step_size=100000,
                        )
                        hq_expressions.reverse()

                        for j, hq in enumerate(hq_expressions):
                            yield DynamicOutput(
                                value=(table, hq, hq_projection),
                                output_name="dynamic_query",
                                mapping_key=f"{table.name}_hq_{j}",
                            )
                    else:
                        constraint_rules = get_constraint_rules(
                            selector=selector, year_id=year_id
                        )
                        constraint_values = get_constraint_values(
                            selector=selector,
                            value=value,
                            step_size=constraint_rules["step_size"],
                        )
                        composed_query = get_query_expression(
                            selector=selector, **constraint_values
                        )

                        yield DynamicOutput(
                            value=(table, composed_query, fq_projection),
                            output_name="dynamic_query",
                            mapping_key=f"{table.name}_q_{i}",
                        )
        else:
            yield DynamicOutput(
                value=(table, None, projection),
                output_name="dynamic_query",
                mapping_key=table.name,
            )


@op(
    ins={"dynamic_query": In(dagster_type=Tuple)},
    out={
        "table": Out(dagster_type=Any, is_required=False),
        "query": Out(dagster_type=Optional[String], is_required=False),
        "projection": Out(dagster_type=Optional[String], is_required=False),
        "count": Out(dagster_type=Int, is_required=False),
        "no_count": Out(dagster_type=Nothing, is_required=False),
    },
    retry_policy=RetryPolicy(max_retries=1, delay=1, backoff=Backoff.EXPONENTIAL),
)
def get_count(context, dynamic_query):
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
        "count": In(dagster_type=Int),
        "query": In(dagster_type=Optional[String]),
        "projection": In(dagster_type=Optional[String]),
    },
    out={"data": Out(dagster_type=List[Dict], io_manager_key="gcs_io")},
    retry_policy=RetryPolicy(max_retries=1, delay=60, backoff=Backoff.EXPONENTIAL),
    config_schema={"query_timeout": Field(Int, is_required=False, default_value=1800)},
)
def get_data(context, table, count, query, projection):
    file_key_parts = [table.name, str(query or "")]
    file_stem = "_".join(filter(None, file_key_parts))
    file_ext = "json.gz"
    file_key = f"{file_key_parts[0]}/{file_stem}.{file_ext}"

    context.log.debug(f"{table.name}\n{query}\n{projection}")

    try:
        with time_limit(context.op_config["query_timeout"]):
            data = table.query(q=query, projection=projection)
    except TimeoutError as e:
        context.log.debug(e)
        raise RetryRequested() from e
    except ConnectionError as e:
        context.log.debug(e)
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
