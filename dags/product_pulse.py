from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["product-pulse"],
)
def product_pulse():

    @task
    def extract():
        # Pretend we pulled this from an API or a file
        return [
            {"product": "Widget", "views": 340},
            {"product": "Gadget", "views": 85},
            {"product": "Gizmo", "views": 512},
        ]

    @task
    def transform(rows):
        # Flag "hot" products (more than 100 views)
        return [{**r, "hot": r["views"] > 100} for r in rows]

    @task
    def load(rows):
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run("""
            CREATE TABLE IF NOT EXISTS product_pulse (
                product TEXT,
                views INTEGER,
                hot BOOLEAN
            );
        """)
        for r in rows:
            hook.run(
                "INSERT INTO product_pulse (product, views, hot) VALUES (%s, %s, %s);",
                parameters=(r["product"], r["views"], r["hot"]),
            )

    load(transform(extract()))

product_pulse()