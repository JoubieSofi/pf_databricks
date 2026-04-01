"""
Fetch column names, table names, and data types from Databricks.

Requirements:
    pip install databricks-sql-connector

Environment variables:
    DATABRICKS_HOST        - e.g. adb-1234567890.1.azuredatabricks.net
    DATABRICKS_TOKEN       - personal access token
    DATABRICKS_HTTP_PATH   - e.g. /sql/1.0/warehouses/<warehouse-id>
"""

import os
from databricks import sql


def get_column_metadata(
    catalog: str | None = None,
    schema: str | None = None,
) -> list[dict]:
    """
    Return a list of dicts with keys: catalog, schema, table, column, data_type.

    Args:
        catalog: Filter to a specific catalog (None = all accessible catalogs).
        schema:  Filter to a specific schema/database (None = all schemas).
    """
    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]

    filters = []
    params = []

    if catalog:
        filters.append("table_catalog = ?")
        params.append(catalog)
    if schema:
        filters.append("table_schema = ?")
        params.append(schema)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            column_name,
            data_type
        FROM information_schema.columns
        {where_clause}
        ORDER BY table_catalog, table_schema, table_name, ordinal_position
    """

    with sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params or None)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

    return [dict(zip(columns, row)) for row in rows]


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Get Databricks column metadata")
    parser.add_argument("--catalog", help="Filter by catalog name")
    parser.add_argument("--schema", help="Filter by schema/database name")
    parser.add_argument("--output", choices=["json", "table"], default="table")
    args = parser.parse_args()

    metadata = get_column_metadata(catalog=args.catalog, schema=args.schema)

    if args.output == "json":
        print(json.dumps(metadata, indent=2))
    else:
        header = f"{'CATALOG':<30} {'SCHEMA':<30} {'TABLE':<40} {'COLUMN':<40} {'DATA_TYPE':<20}"
        print(header)
        print("-" * len(header))
        for row in metadata:
            print(
                f"{row['table_catalog']:<30} "
                f"{row['table_schema']:<30} "
                f"{row['table_name']:<40} "
                f"{row['column_name']:<40} "
                f"{row['data_type']:<20}"
            )
        print(f"\n{len(metadata)} column(s) found.")
