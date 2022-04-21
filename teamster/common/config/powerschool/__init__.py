import os

from dagster import Field, Array, Shape, Any, IntSource, String

PS_QUERY_CONFIG = {
    "tables": Field(
        Array(
            Shape(
                {
                    "name": String,
                    "queries": Field(
                        Array(
                            Shape(
                                {
                                    "projection": Field(String, is_required=False),
                                    "q": Field(
                                        Shape(
                                            {
                                                "selector": String,
                                                "value": Field(Any, is_required=False),
                                            }
                                        ),
                                        is_required=False,
                                    ),
                                }
                            )
                        ),
                        is_required=False,
                    ),
                }
            )
        )
    ),
    "year_id": Field(
        IntSource,
        is_required=False,
        default_value=int(os.getenv("POWERSCHOOL_YEAR_ID")),
    ),
}
