from dagster import op
from powerschool import PowerSchool, utils


@op(required_resource_keys={"google_secret"})
def hello(context):
    secret_name = f"projects/624231820004/secrets/spam/versions/1"
    secret_value = context.resources.google_secret(secret_name)
    context.log.info(secret_value)


@op
def authenticate(context):
    return PowerSchool(
        host=context.op_config["hostname"], auth=context.op_config["auth"]
    )


"""# save access token to secret
with token_file_path.open("wt") as f:
    json.dump(ps.access_token, f)
    f.truncate()

# for t in tables:
table_name = t.get("table_name")
projection = t.get("projection")
queries = t.get("queries")
print(table_name)

# create data folder
file_dir = PROJECT_PATH / "data" / host_clean / table_name
if not file_dir.exists():
    file_dir.mkdir(parents=True)
    print(f"\tCreated {file_dir}...")

# get table
schema_table = ps.get_schema_table(table_name)

# if there are queries, generate FIQL
query_params = []
if queries:
    selector = queries.get("selector")
    values = queries.get("values")

    # check if data exists for specified table
    if not [f for f in file_dir.iterdir()]:
        # generate historical queries
        print("\tNo existing data. Generating historical queries...")
        query_params = utils.generate_historical_queries(
            current_yearid, selector
        )
        query_params.reverse()
    else:
        constraint_rules = utils.get_constraint_rules(selector, current_yearid)

        # if there aren't specified values, transform yearid to value
        if not values:
            values = [utils.transform_yearid(current_yearid, selector)]

        # for each value, get query expression
        for v in values:
            if v == "yesterday":
                today = datetime.date.today()
                yesterday = today - datetime.timedelta(days=1)
                expression = f"{selector}=ge={yesterday.isoformat()}"
            else:
                constraint_values = utils.get_constraint_values(
                    selector, v, constraint_rules["step_size"]
                )
                expression = utils.get_query_expression(
                    selector, **constraint_values
                )

            query_params.append(expression)
else:
    query_params.append({})

for q in query_params:
    q_params = {}
    if q:
        print(f"\tQuerying {q} ...")
        q_params["q"] = q
        file_name = f"{table_name}_{q}.json.gz"
    else:
        print("\tQuerying all records...")
        file_name = f"{table_name}.json.gz"
    
    file_path = file_dir / file_name

    count = schema_table.count(**q_params)
    print(f"\t\tFound {count} records!")

    if count > 0:
        if projection:
            q_params["projection"] = projection

        data = schema_table.query(**q_params)

        # save as json.gz
        with gzip.open(file_path, "wt", encoding="utf-8") as f:
            json.dump(data, f)
        print(f"\t\tSaved to {file_path}!")

        # upload to GCS
        file_path_parts = file_path.parts
        destination_blob_name = f"powerschool/{'/'.join(file_path_parts[file_path_parts.index('data') + 1:])}"
        blob = gcs_bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
        print(f"\t\tUploaded to {blob.public_url}!")
"""
