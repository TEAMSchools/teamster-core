import gzip
import json
import math
import re

from dagster import (
    Any,
    Bool,
    DynamicOut,
    DynamicOutput,
    Field,
    In,
    Int,
    Nothing,
    Optional,
    Out,
    Output,
    RetryPolicy,
    RetryRequested,
    String,
    Tuple,
    op,
)
from dagster.core.types.dagster_type import List
from powerschool.utils import (
    generate_historical_queries,
    get_constraint_rules,
    get_constraint_values,
    get_query_expression,
    transform_year_id,
)
from requests.exceptions import HTTPError

from teamster.common.config.powerschool import COMPOSE_QUERIES_CONFIG
from teamster.common.utils import TODAY, time_limit, get_last_schedule_run


@op(
    ins={"table_resyncs": In(dagster_type=List[Tuple])},
    out={"dynamic_tables": DynamicOut(dagster_type=Tuple, is_required=False)},
    config_schema={
        "step_size": Field(Int, is_required=False, default_value=30000),
        "force": Field(Bool, is_required=False, default_value=False),
    },
    tags={"dagster/priority": 1},
)
def compose_resyncs(context, table_resyncs):
    for tr in table_resyncs:
        year_id, table, projection, selector, max_value = tr

        context.log.info(f"Generating historical queries for {table.name}.")

        if not max_value and selector[-2:] == "id":
            max_value = int(
                table.query(
                    projection=selector,
                    sort=selector,
                    sortdescending="true",
                    pagesize=1,
                    page=1,
                )[0][selector]
            )
            place_value = 10 ** (len(str(max_value)) - 1)
            max_val_ceil = math.ceil(max_value / place_value) * place_value
            max_value = max_val_ceil
        elif not max_value:
            max_value = transform_year_id(year_id, selector)
        context.log.debug(f"max_value:\t{max_value}")
        constraint_rules = get_constraint_rules(
            selector, year_id=year_id, is_historical=True
        )

        historical_queries = generate_historical_queries(
            selector=selector,
            start_value=max_value,
            stop_value=constraint_rules["stop_value"],
            step_size=context.op_config["step_size"],
        )
        historical_queries.reverse()

        for i, hq in enumerate(historical_queries):
            mapping_key = f"{re.sub(r'[^A-Za-z0-9]', '_', table.name)}_h_{i}"
            yield DynamicOutput(
                value=(table, projection, hq, True),
                output_name="dynamic_tables",
                mapping_key=mapping_key,
            )


@op(
    ins={"table_queries": In(dagster_type=List[Tuple])},
    out={"dynamic_tables": DynamicOut(dagster_type=Tuple, is_required=False)},
    tags={"dagster/priority": 2},
)
def compose_queries(context, table_queries):
    for ftq in table_queries:
        year_id, table, mapping_key, projection, selector, value = ftq

        constraint_rules = get_constraint_rules(selector=selector, year_id=year_id)

        constraint_values = get_constraint_values(
            selector=selector,
            value=value,
            step_size=constraint_rules["step_size"],
        )

        composed_query = get_query_expression(selector=selector, **constraint_values)

        yield DynamicOutput(
            value=(table, projection, composed_query, False),
            output_name="dynamic_tables",
            mapping_key=mapping_key,
        )


@op(
    ins={"table_queries": In(dagster_type=List[Tuple])},
    out={
        "dynamic_tables": DynamicOut(dagster_type=Tuple, is_required=False),
        "table_queries": Out(dagster_type=List[Tuple], is_required=False),
        "table_resyncs": Out(dagster_type=List[Tuple], is_required=False),
    },
    tags={"dagster/priority": 3},
)
def filter_queries(context, table_queries):
    table_queries_filtered = []
    table_resyncs = []

    for tbl in table_queries:
        year_id, table, projection, queries = tbl

        for i, query in enumerate(queries):
            mapping_key = f"{re.sub(r'[^A-Za-z0-9]', '_', table.name)}_q_{i}"

            projection = query.get("projection", projection)
            q = query.get("q")

            if isinstance(q, str):
                yield DynamicOutput(
                    value=(table, projection, q, False),
                    output_name="dynamic_tables",
                    mapping_key=mapping_key,
                )
            else:
                selector = q["selector"]
                max_value = q.get("max_value")
                value = q.get("value", transform_year_id(year_id, selector))

                if value == "today":
                    value = TODAY.date().isoformat()

                if value == "resync":
                    table_resyncs.append(
                        (year_id, table, projection, selector, max_value)
                    )
                else:
                    table_queries_filtered.append(
                        (year_id, table, mapping_key, projection, selector, value)
                    )

    if table_queries_filtered:
        yield Output(value=table_queries_filtered, output_name="table_queries")

    if table_resyncs:
        yield Output(value=table_resyncs, output_name="table_resyncs")


@op(
    config_schema=COMPOSE_QUERIES_CONFIG,
    out={
        "dynamic_tables": DynamicOut(dagster_type=Tuple, is_required=False),
        "table_queries": Out(dagster_type=List[Tuple], is_required=False),
    },
    required_resource_keys={"powerschool"},
    tags={"dagster/priority": 4},
)
def compose_tables(context):
    tables = context.op_config["tables"]
    year_id = context.op_config["year_id"]

    table_queries = []
    for i, tbl in enumerate(tables):
        table = context.resources.powerschool.get_schema_table(tbl["name"])
        projection = tbl.get("projection")
        queries = [fq for fq in tbl.get("queries", {}) if fq.get("q")]

        mapping_key = f"{re.sub(r'[^A-Za-z0-9]', '_', table.name)}_t_{i}"

        if queries:
            table_queries.append((year_id, table, projection, queries))
        else:
            yield DynamicOutput(
                value=(table, projection, None, False),
                output_name="dynamic_tables",
                mapping_key=mapping_key,
            )

    if table_queries:
        yield Output(value=table_queries, output_name="table_queries")


def table_count(context, table, query):
    try:
        return table.count(q=query)
    except Exception as e:
        context.log.error(e)
        raise e


def time_limit_count(context, table, query, count_type="query", is_resync=False):
    if count_type == "updated" and is_resync:
        # proceed to original query count
        context.log.info("Resync - Skipping transaction_date count.")
        return 1
    elif count_type == "updated":
        last_run_datetime = get_last_schedule_run(context)
        if last_run_datetime:
            last_run_date = last_run_datetime.date().isoformat()
        else:
            # proceed to original query count
            context.log.info("Ad Hoc - Skipping transaction_date count.")
            return 1

        context.log.info(
            f"Searching for matching records updated since {last_run_date}."
        )

        query = ";".join(
            [
                f"transaction_date=ge={last_run_date}",
                str(query or ""),
            ]
        )

    with time_limit(context.op_config["query_timeout"]):
        try:
            return table_count(context=context, table=table, query=query)
        except HTTPError as e:
            if str(e) == '{"message":"Invalid field transaction_date"}':
                # proceed to original query count
                context.log.info("Skipping transaction_date count.")
                return 1
            else:
                raise e


@op(
    ins={"table_query": In(dagster_type=Tuple)},
    out={
        "table": Out(dagster_type=Any, is_required=False),
        "projection": Out(dagster_type=Optional[String], is_required=False),
        "query": Out(dagster_type=Optional[String], is_required=False),
        "count": Out(dagster_type=Int, is_required=False),
        "n_pages": Out(dagster_type=Int, is_required=False),
        "is_resync": Out(dagster_type=Bool, is_required=False),
        "no_count": Out(dagster_type=Nothing, is_required=False),
    },
    retry_policy=RetryPolicy(max_retries=9, delay=30),
    config_schema={"query_timeout": Field(Int, is_required=False, default_value=30)},
    tags={"dagster/priority": 5},
)
def get_count(context, table_query):
    table, projection, query, is_resync = table_query
    context.log.info(
        f"table:\t\t{table.name}\nprojection:\t{projection}\nq:\t\t{query}"
    )

    try:
        # count query records updated since last run
        updated_count = time_limit_count(
            context=context,
            table=table,
            query=query,
            count_type="updated",
            is_resync=is_resync,
        )
    except Exception as e:
        raise RetryRequested(
            max_retries=context.op_def.retry_policy.max_retries,
            seconds_to_wait=context.op_def.retry_policy.delay,
        ) from e

    if updated_count > 0:
        try:
            # count all records in query
            query_count = time_limit_count(context=context, table=table, query=query)
        except Exception as e:
            raise RetryRequested(
                max_retries=context.op_def.retry_policy.max_retries,
                seconds_to_wait=context.op_def.retry_policy.delay,
            ) from e
    else:
        context.log.info("No record updates since last run. Skipping.")
        return Output(value=None, output_name="no_count")

    context.log.info(f"count:\t{query_count}")
    if query_count > 0:
        n_pages = math.ceil(
            query_count / table.client.metadata.schema_table_query_max_page_size
        )
        context.log.info(f"total pages:\t{n_pages}")

        yield Output(value=table, output_name="table")
        yield Output(value=query, output_name="query")
        yield Output(value=projection, output_name="projection")
        yield Output(value=query_count, output_name="count")
        yield Output(value=n_pages, output_name="n_pages")
        yield Output(value=is_resync, output_name="is_resync")
    else:
        return Output(value=None, output_name="no_count")


def table_query(context, table, query, projection, page):
    try:
        return table.query(q=query, projection=projection, page=page)
    except Exception as e:
        context.log.error(e)
        raise e


def time_limit_query(context, table, query, projection, page, retry=False):
    with time_limit(context.op_config["query_timeout"]):
        try:
            return table_query(
                context=context,
                table=table,
                query=query,
                projection=projection,
                page=page,
            )
        except Exception as e:
            if retry:
                raise e
            else:
                # retry page before retrying entire Op
                return time_limit_query(
                    context=context,
                    table=table,
                    query=query,
                    projection=projection,
                    page=page,
                    retry=True,
                )


@op(
    ins={
        "table": In(dagster_type=Any),
        "projection": In(dagster_type=Optional[String]),
        "query": In(dagster_type=Optional[String]),
        "count": In(dagster_type=Int),
        "n_pages": In(dagster_type=Int),
        "is_resync": In(dagster_type=Bool),
    },
    out={"gcs_file_handles": Out(dagster_type=List)},
    required_resource_keys={"gcs_fm"},
    retry_policy=RetryPolicy(max_retries=9, delay=30),
    config_schema={"query_timeout": Field(Int, is_required=False, default_value=30)},
    tags={"dagster/priority": 6},
)
def get_data(context, table, projection, query, count, n_pages, is_resync):
    file_ext = "json.gz"
    file_stem = "_".join(filter(None, [table.name, str(query or "")]))

    data_len = 0
    gcs_file_handles = []
    for p in range(n_pages):
        context.log.debug(f"page:\t{(p + 1)}/{n_pages}")

        file_key = f"{table.name}/{file_stem}_p_{p}.{file_ext}"
        # TODO: change to context.retry_number > 0?
        if is_resync and context.resources.gcs_fm._has_object(key=file_key):
            continue
        else:
            try:
                data = time_limit_query(
                    context=context,
                    table=table,
                    query=query,
                    projection=projection,
                    page=(p + 1),
                )
            except Exception as e:
                raise RetryRequested(
                    max_retries=context.op_def.retry_policy.max_retries,
                    seconds_to_wait=context.op_def.retry_policy.delay,
                ) from e

            jsongz_obj = gzip.compress(json.dumps(data).encode("utf-8"))

            gcs_file_handles.append(
                context.resources.gcs_fm.upload_data(obj=jsongz_obj, file_key=file_key)
            )
            data_len += len(data)

    if data_len != count:
        context.log.warning(
            (
                "Received different number of records than expected: "
                f"{data_len} < {count}."
                "Recounting."
            )
        )

        updated_count = table.count(q=query)
        if data_len != updated_count:
            context.log.warning(
                (
                    "Record count still different than expected: "
                    f"{data_len} < {count}."
                    "Retrying."
                )
            )
            raise RetryRequested(
                max_retries=context.op_def.retry_policy.max_retries,
                seconds_to_wait=context.op_def.retry_policy.delay,
            )

    return Output(value=gcs_file_handles, output_name="gcs_file_handles")
