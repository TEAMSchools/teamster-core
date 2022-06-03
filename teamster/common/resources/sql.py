import datetime
import decimal
import json

from dagster import resource, StringSource, IntSource, Field
from dagster.utils.merger import merge_dicts
from sqlalchemy import text
from sqlalchemy.engine import URL, create_engine


class SqlAlchemyJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime.timedelta, decimal.Decimal)):
            return str(o)
        elif isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()
        else:
            return super().default(o)


class SqlAlchemyEngine(object):
    def __init__(self, dialect, driver, logger, **kwargs):
        self.log = logger
        self.connection_url = URL.create(drivername=f"{dialect}+{driver}", **kwargs)
        self.engine = create_engine(url=self.connection_url)

        self.log.info(self.connection_url)

    def execute_text_query(self, query, output="dict"):
        with self.engine.connect() as conn:
            result = conn.execute(statement=text(query))

            if output in ["dict", "json"]:
                output_obj = [dict(row) for row in result.mappings()]
            else:
                output_obj = [row for row in result]

        if output == "json":
            return json.dumps(obj=output_obj, cls=SqlAlchemyJsonEncoder)
        else:
            return output_obj


class MssqlEngine(SqlAlchemyEngine):
    def __init__(self, dialect, driver, logger, mssql_driver, **kwargs):
        super().__init__(
            dialect, driver, logger, query={"driver": mssql_driver}, **kwargs
        )


SQLALCHEMY_ENGINE_CONFIG = {
    "dialect": Field(StringSource),
    "driver": Field(StringSource),
    "username": Field(StringSource, is_required=False),
    "password": Field(StringSource, is_required=False),
    "host": Field(StringSource, is_required=False),
    "port": Field(IntSource, is_required=False),
    "database": Field(StringSource, is_required=False),
}


@resource(
    config_schema=merge_dicts(
        SQLALCHEMY_ENGINE_CONFIG,
        {"mssql_driver": Field(StringSource, is_required=False)},
    )
)
def mssql(context):
    return MssqlEngine(logger=context.log, **context.resource_config)
