import pandas as pd
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


def do_something():
    mysql_engine = MySqlHook(
        mysql_conn_id='trideptrai', use_unicode=True
    ).get_sqlalchemy_engine()
    # mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN, use_unicode=True)
    big_query_hook = BigQueryHook(
        bigquery_conn_id='bigquery_staging', use_legacy_sql=False
    )

    get_something(mysql_engine, big_query_hook)


def get_something(mysql_engine, big_query_hook):
    query = f"""
        SELECT
            id,
            product_name
        FROM test.products c
    """
    df = pd.read_sql_query(query, mysql_engine)

    all_data = df.to_dict("records")

    if len(all_data) > 0:
        big_query_hook.insert_all(
            project_id='bi-stg-intrepid',
            dataset_id='intrepid_test',
            table_id='test_collation',
            rows=all_data,
            fail_on_error=True,
        )
