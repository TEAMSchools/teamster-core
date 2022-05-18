import gzip
import json
import math
import pathlib
import re
import shutil

from dagster import (
    Any,
    Backoff,
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
from powerschool.utils import (
    generate_historical_queries,
    get_constraint_rules,
    get_constraint_values,
    get_query_expression,
    transform_year_id,
)
from requests.exceptions import HTTPError

from teamster.common.config.powerschool import COMPOSE_QUERIES_CONFIG
from teamster.common.utils import TODAY, time_limit, gql_last_schedule_run


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

        table_name = re.sub("[^A-Za-z0-9]", "_", table.name)
        filtered_queries = [fq for fq in queries if fq.get("q")]

        if filtered_queries:
            for i, fq in enumerate(filtered_queries):
                fq_projection = fq.get("projection", projection)
                q = fq.get("q")

                if isinstance(q, str):
                    yield DynamicOutput(
                        value=(table, q, fq_projection, False),
                        output_name="dynamic_query",
                        mapping_key=f"{table_name}_q_{i}",
                    )
                else:
                    selector = q.get("selector")
                    value = q.get("value", transform_year_id(year_id, selector))

                    if value == "today":
                        value = TODAY.date().isoformat()

                    if value == "resync":
                        context.log.info(
                            f"Generating historical queries for {table_name}."
                        )

                        max_value = q.get("max_value")
                        if not max_value and selector[-2:] == "id":
                            max_value = int(
                                table.count() * 1.5
                            )  # set max historical value for "id" queries to 1.5x count
                        elif not max_value:
                            max_value = transform_year_id(year_id, selector)
                        context.log.debug(f"max_value:\t{max_value}")

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
                                value=(table, hq, fq_projection, True),
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
                            value=(table, composed_query, fq_projection, False),
                            output_name="dynamic_query",
                            mapping_key=f"{table_name}_q_{i}",
                        )
        else:
            yield DynamicOutput(
                value=(table, None, projection, False),
                output_name="dynamic_query",
                mapping_key=table_name,
            )


def table_count(context, table, query):
    try:
        return table.count(q=query)
    except Exception as e:
        context.log.error(e)
        raise e


def time_limit_count(context, table, query, count_type="query", is_resync=False):
    # TODO: make relative date last run from schedule
    context.log.debug(gql_last_schedule_run(context))
    if is_resync:
        # proceed to original query count
        context.log.info("Resync - Skipping transaction_date count.")
        return 1
    elif count_type == "transaction":
        query = ";".join(
            [
                "transaction_date=ge="
                + gql_last_schedule_run(context).date().isoformat(),
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
    ins={"dynamic_query": In(dagster_type=Tuple)},
    out={
        "table": Out(dagster_type=Any, is_required=False),
        "query": Out(dagster_type=Optional[String], is_required=False),
        "projection": Out(dagster_type=Optional[String], is_required=False),
        "count": Out(dagster_type=Int, is_required=False),
        "n_pages": Out(dagster_type=Int, is_required=False),
        "no_count": Out(dagster_type=Nothing, is_required=False),
    },
    retry_policy=RetryPolicy(max_retries=1, delay=1, backoff=Backoff.EXPONENTIAL),
    config_schema={"query_timeout": Field(Int, is_required=False, default_value=60)},
)
def get_count(context, dynamic_query):
    table, query, projection, is_resync = dynamic_query

    context.log.info(
        f"table:\t\t{table.name}\nq:\t\t{query}\nprojection:\t{projection}\n"
    )

    try:
        # count query records updated since last run
        transaction_count = time_limit_count(
            context=context,
            table=table,
            query=query,
            count_type="transaction",
            is_resync=is_resync,
        )
    except Exception as e:
        raise RetryRequested() from e

    if transaction_count > 0:
        try:
            # count all records in query
            query_count = time_limit_count(context=context, table=table, query=query)
        except Exception as e:
            raise RetryRequested() from e

        n_pages = math.ceil(
            query_count / table.client.metadata.schema_table_query_max_page_size
        )

        context.log.info(f"count:\t\t{query_count}\ntotal pages:\t{n_pages}")

        yield Output(value=table, output_name="table")
        yield Output(value=query, output_name="query")
        yield Output(value=projection, output_name="projection")
        yield Output(value=query_count, output_name="count")
        yield Output(value=n_pages, output_name="n_pages")
    else:
        yield Output(value=None, output_name="no_count")


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
        "n_pages": In(dagster_type=Int),
        "query": In(dagster_type=Optional[String]),
        "projection": In(dagster_type=Optional[String]),
    },
    out={"gcs_path": Out(dagster_type=String)},
    required_resource_keys={"gcs_fm"},
    retry_policy=RetryPolicy(max_retries=1, delay=60, backoff=Backoff.EXPONENTIAL),
    config_schema={"query_timeout": Field(Int, is_required=False, default_value=60)},
    tags={"dagster/priority": 1},
)
def get_data(context, table, query, projection, count, n_pages):
    data_dir = pathlib.Path("data").absolute()

    file_dir = data_dir / table.name
    if not file_dir.exists():
        file_dir.mkdir(parents=True)
        context.log.info(f"Created folder {file_dir}.")

    file_ext = "json"
    file_key_parts = [table.name, str(query or "")]

    file_stem = "_".join(filter(None, file_key_parts))

    file_key = f"{table.name}/{file_stem}.{file_ext}"

    data_len = 0
    for p in range(n_pages):
        context.log.debug(
            f"table:\t{table.name}\nq:\t{query}\npage:\t{(p + 1)}/{n_pages}"
        )

        try:
            data = time_limit_query(
                context=context,
                table=table,
                query=query,
                projection=projection,
                page=(p + 1),
            )
        except Exception as e:
            raise RetryRequested() from e

        data_len += len(data)

        tmp_file_path = data_dir / file_key
        if p == 0:
            with tmp_file_path.open(mode="wt", encoding="utf-8") as f_tmp:
                json.dump(data, f_tmp)
        else:
            with tmp_file_path.open(mode="at", encoding="utf-8") as f_tmp:
                f_tmp.seek(0, 2)
                position = f_tmp.tell() - 1
                f_tmp.seek(position)
                f_tmp.write(f", {json.dumps(data)[1:-1]}]")

    if data_len < count:
        updated_count = table.count(q=query)
        if data_len < updated_count:
            raise RetryRequested(
                f"Data received is less than count: {data_len} < {count}"
            )
    else:
        gz_file_path = data_dir / (file_key + ".gz")
        with tmp_file_path.open(mode="rt", encoding="utf-8") as f_tmp:
            with gzip.open(gz_file_path, mode="wt", encoding="utf-8") as f_gz:
                shutil.copyfileobj(f_tmp, f_gz)

        gcs_fh = context.resources.gcs_fm.upload(
            gz_file_path, file_key=(file_key + ".gz")
        )

        yield Output(value=gcs_fh.gcs_path, output_name="gcs_path")
