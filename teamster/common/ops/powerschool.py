from dagster import DynamicOut, DynamicOutput, Out, Output, op

from powerschool.utils import (
    generate_historical_queries,
    get_constraint_rules,
    get_constraint_values,
    get_query_expression,
    transform_yearid,
)


@op(required_resource_keys={"powerschool"})
def get_ps_client(context):
    return context.resources.powerschool


@op(
    config_schema={"year_id": int, "tables": list},
    out={"query": DynamicOut()},
    required_resource_keys={"gcs_fm"},
)
def compose_queries(context):
    year_id = context.op_config["year_id"]
    tables = context.op_config["tables"]

    for tbl in tables:
        table_name = tbl["name"]
        queries = tbl.get("queries", {})

        filtered_queries = [fq for fq in queries if fq.get("q")]

        if filtered_queries:
            for i, fq in enumerate(filtered_queries):
                fq_projection = fq.get("projection")
                q = fq.get("q")

                selector = q.get("selector")
                value = q.get("value", transform_yearid(year_id, selector))

                constraint_rules = get_constraint_rules(selector, year_id)
                constraint_values = get_constraint_values(
                    selector, value, constraint_rules["step_size"]
                )
                composed_query = get_query_expression(selector, **constraint_values)

                yield DynamicOutput(
                    value=(table_name, composed_query, fq_projection),
                    output_name="query",
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
                        output_name="query",
                        mapping_key=f"{table_name}_hq_{j}",
                    )
        else:
            projection = next(
                iter([pj["projection"] for pj in queries if pj.get("projection")]), None
            )

            yield DynamicOutput(
                value=(table_name, None, projection),
                output_name="query",
                mapping_key=table_name,
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
