from dagster import op
from dagster import Any, Dict, Int, List, Nothing, Optional, String, Tuple
from dagster import In, Out, DynamicOut, Output, DynamicOutput
from powerschool.utils import (
    generate_historical_queries,
    get_constraint_rules,
    get_constraint_values,
    get_query_expression,
    transform_year_id,
)

from teamster.common.config.powerschool import PS_QUERY_CONFIG


@op(
    config_schema=PS_QUERY_CONFIG,
    out={"dynamic_query": DynamicOut(dagster_type=Tuple)},
    required_resource_keys={"gcs_fm"},
)
def compose_queries(context):
    tables = context.op_config["tables"]
    year_id = context.op_config.get("year_id")

    for tbl in tables:
        table_name = tbl["name"]
        queries = tbl.get("queries", {})

        filtered_queries = [fq for fq in queries if fq.get("q")]

        if filtered_queries:
            for i, fq in enumerate(filtered_queries):
                fq_projection = fq.get("projection")
                q = fq.get("q")

                if isinstance(q, str):
                    composed_query = q
                else:
                    selector = q.get("selector")
                    value = q.get("value", transform_year_id(year_id, selector))

                    constraint_rules = get_constraint_rules(selector, year_id)
                    constraint_values = get_constraint_values(
                        selector, value, constraint_rules["step_size"]
                    )
                    composed_query = get_query_expression(selector, **constraint_values)

                yield DynamicOutput(
                    value=(table_name, composed_query, fq_projection),
                    output_name="dynamic_query",
                    mapping_key=f"{table_name}_q_{i}",
                )

            # check if no data exists and generate historical queries
            if not context.resources.gcs_fm.blob_exists(file_key=table_name):
                context.log.info(
                    f"No data. Generating historical queries for {table_name}"
                )

                hq_projection = next(
                    iter([pj["projection"] for pj in queries if pj.get("projection")]),
                    None,
                )

                hist_query_exprs = generate_historical_queries(year_id, selector)
                hist_query_exprs.reverse()

                for j, hq in enumerate(hist_query_exprs):
                    yield DynamicOutput(
                        value=(table_name, hq, hq_projection),
                        output_name="dynamic_query",
                        mapping_key=f"{table_name}_hq_{j}",
                    )
        else:
            projection = next(
                iter([pj["projection"] for pj in queries if pj.get("projection")]), None
            )

            yield DynamicOutput(
                value=(table_name, None, projection),
                output_name="dynamic_query",
                mapping_key=table_name,
            )


@op(
    ins={"dynamic_query": In(dagster_type=Tuple)},
    out={
        "table_name": Out(dagster_type=String),
        "query": Out(dagster_type=Optional[String]),
        "projection": Out(dagster_type=Optional[String]),
    },
)
def split_dynamic_output(dynamic_query):
    table_name, query, projection = dynamic_query

    yield Output(value=table_name, output_name="table_name")
    yield Output(value=query, output_name="query")
    yield Output(value=projection, output_name="projection")


@op(
    ins={"table_name": In(dagster_type=String)},
    out={"table": Out(dagster_type=Any)},
    required_resource_keys={"powerschool"},
)
def get_table(context, table_name):
    table = context.resources.powerschool.get_schema_table(table_name)
    yield Output(value=table, output_name="table")


@op(
    ins={"table": In(dagster_type=Any), "query": In(dagster_type=Optional[String])},
    out={
        "count": Out(dagster_type=Int, is_required=False),
        "no_data": Out(dagster_type=Nothing, is_required=False),
    },
)
def query_count(context, table, query):
    context.log.debug(f"{table.name}?q={query}")

    count = table.count(q=query)
    context.log.info(f"Found {count} records")

    if count > 0:
        yield Output(value=count, output_name="count")
    else:
        yield Output(value=None, output_name="no_data")


@op(
    ins={
        "table": In(dagster_type=Any),
        "query": In(dagster_type=Optional[String]),
        "projection": In(dagster_type=Optional[String]),
        "count": In(dagster_type=Int),
    },
    out={
        "data": Out(
            dagster_type=List[Dict], is_required=False, io_manager_key="gcs_io"
        ),
        "count_error": Out(dagster_type=Nothing, is_required=False),
    },
)
def query_data(context, table, query, projection, count):
    file_key_parts = [table.name, str(query or "")]
    file_stem = "_".join(filter(None, file_key_parts))
    file_ext = "json.gz"
    file_key = f"{file_key_parts[0]}/{file_stem}.{file_ext}"

    context.log.debug(f"{table.name}\n{query}\n{projection}")
    data = table.query(q=query, projection=projection)

    len_data = len(data)
    if len_data < count:
        updated_count = table.count(q=query)
        if len_data < updated_count:
            yield Output(value=None, output_name="count_error")  # TODO: raise exception
    else:
        yield Output(value=data, output_name="data", metadata={"file_key": file_key})
