from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.models import Variable


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fashion_store_ingestion",
    default_args=default_args,
    description="Ingestion des ventes fashion store depuis Minio vers PostgreSQL",
    schedule=None,
    start_date=datetime(2025, 4, 4),
    catchup=False,
    tags=["fashion", "ingestion"],
    params={"ingestion_date": "20250616"},
) as dag:

    @task()
    def extract_from_minio(**context):
        import boto3
        import io
        import pandas as pd
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection("minio_s3")
        extra = conn.extra_dejson
        endpoint_url = extra.get("endpoint_url", "http://minio:9000")
        access_key = extra.get("aws_access_key_id", "minioadmin")
        secret_key = extra.get("aws_secret_access_key", "minioadmin123")

        bucket = Variable.get("minio_bucket", default_var="folder-source")
        csv_key = Variable.get("minio_csv_key", default_var="fashion_store_sales.csv")

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        response = s3.get_object(Bucket=bucket, Key=csv_key)
        df = pd.read_csv(io.BytesIO(response["Body"].read()))
        return df.to_json(orient="records", date_format="iso")

    @task()
    def transform(csv_json, **context):
        import pandas as pd
        import io
        from datetime import datetime as dt

        date_str = context["params"].get("ingestion_date", "20250616")
        target_date = dt.strptime(date_str, "%Y%m%d").date()

        df = pd.read_json(io.StringIO(csv_json), orient="records")
        df["sale_date"] = pd.to_datetime(df["sale_date"]).dt.date
        filtered = df[df["sale_date"] == target_date].copy()

        if filtered.empty:
            return {"empty": True}

        result = {"empty": False}

        result["countries"] = filtered[["country"]].drop_duplicates().rename(
            columns={"country": "country_name"}).to_json(orient="records")
        result["categories"] = filtered[["category"]].drop_duplicates().rename(
            columns={"category": "category_name"}).to_json(orient="records")
        result["brands"] = filtered[["brand"]].drop_duplicates().rename(
            columns={"brand": "brand_name"}).to_json(orient="records")
        result["colors"] = filtered[["color"]].drop_duplicates().rename(
            columns={"color": "color_name"}).to_json(orient="records")

        sizes_df = filtered[["size"]].drop_duplicates().rename(columns={"size": "size_label"})
        sizes_df["size_label"] = sizes_df["size_label"].astype(str)
        result["sizes"] = sizes_df.to_json(orient="records")

        result["age_ranges"] = filtered[["age_range"]].drop_duplicates().rename(
            columns={"age_range": "age_range_label"}).to_json(orient="records")

        result["channels"] = filtered[["channel", "channel_campaigns"]].drop_duplicates().rename(
            columns={"channel": "channel_name", "channel_campaigns": "campaign_name"}).to_json(orient="records")

        customers = filtered.drop_duplicates(subset=["customer_id"])[
            ["customer_id", "first_name", "last_name", "email", "gender",
             "age_range", "signup_date", "country"]].copy()
        result["customers"] = customers.to_json(orient="records")

        products = filtered.drop_duplicates(subset=["product_id"])[
            ["product_id", "product_name", "category", "brand", "color",
             "size", "catalog_price", "cost_price"]].copy()
        products["size"] = products["size"].astype(str)
        result["products"] = products.to_json(orient="records")

        sales = filtered.drop_duplicates(subset=["sale_id"])[
            ["sale_id", "sale_date", "customer_id", "channel"]].copy()
        sales["sale_date"] = sales["sale_date"].astype(str)
        result["sales"] = sales.to_json(orient="records")

        sale_items = filtered[
            ["item_id", "sale_id", "product_id", "quantity",
             "original_price", "discount_applied"]].copy()
        result["sale_items"] = sale_items.to_json(orient="records")

        return result

    @task()
    def load_to_postgres(tables_json, **context):
        import pandas as pd
        import io
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if tables_json.get("empty"):
            print("Aucune donnee pour cette date")
            return

        pg_hook = PostgresHook(postgres_conn_id="postgres_fashion")
        conn = pg_hook.get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        try:
            lookup_tables = {
                "countries": ("country_id", "country_name"),
                "categories": ("category_id", "category_name"),
                "brands": ("brand_id", "brand_name"),
                "colors": ("color_id", "color_name"),
                "sizes": ("size_id", "size_label"),
                "age_ranges": ("age_range_id", "age_range_label"),
            }

            maps = {}
            for table, (id_col, name_col) in lookup_tables.items():
                df = pd.read_json(io.StringIO(tables_json[table]), orient="records")
                for _, row in df.iterrows():
                    cur.execute(
                        f"INSERT INTO {table} ({name_col}) VALUES (%s) ON CONFLICT ({name_col}) DO NOTHING",
                        (str(row.iloc[0]),),
                    )
                cur.execute(f"SELECT {id_col}, {name_col} FROM {table}")
                maps[table] = {r[1]: r[0] for r in cur.fetchall()}

            channels_df = pd.read_json(io.StringIO(tables_json["channels"]), orient="records")
            for _, row in channels_df.iterrows():
                cur.execute(
                    "INSERT INTO channels (channel_name, campaign_name) VALUES (%s, %s) "
                    "ON CONFLICT (channel_name) DO NOTHING",
                    (row["channel_name"], row["campaign_name"]),
                )
            cur.execute("SELECT channel_id, channel_name FROM channels")
            channel_map = {r[1]: r[0] for r in cur.fetchall()}

            customers_df = pd.read_json(io.StringIO(tables_json["customers"]), orient="records")
            for _, row in customers_df.iterrows():
                cur.execute(
                    "INSERT INTO customers "
                    "(customer_id, first_name, last_name, email, gender, age_range_id, signup_date, country_id) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (customer_id) DO UPDATE SET "
                    "first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name, email=EXCLUDED.email",
                    (
                        int(row["customer_id"]),
                        row["first_name"] if pd.notna(row["first_name"]) else None,
                        row["last_name"] if pd.notna(row["last_name"]) else None,
                        row["email"] if pd.notna(row["email"]) else None,
                        row["gender"],
                        maps["age_ranges"][row["age_range"]],
                        row["signup_date"],
                        maps["countries"][row["country"]],
                    ),
                )

            products_df = pd.read_json(io.StringIO(tables_json["products"]), orient="records")
            for _, row in products_df.iterrows():
                cur.execute(
                    "INSERT INTO products "
                    "(product_id, product_name, category_id, brand_id, color_id, size_id, catalog_price, cost_price) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (product_id) DO NOTHING",
                    (
                        int(row["product_id"]),
                        row["product_name"],
                        maps["categories"][row["category"]],
                        maps["brands"][row["brand"]],
                        maps["colors"][row["color"]],
                        maps["sizes"][str(row["size"])],
                        float(row["catalog_price"]),
                        float(row["cost_price"]),
                    ),
                )

            sales_df = pd.read_json(io.StringIO(tables_json["sales"]), orient="records")
            for _, row in sales_df.iterrows():
                cur.execute(
                    "INSERT INTO sales (sale_id, sale_date, customer_id, channel_id) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (sale_id) DO NOTHING",
                    (
                        int(row["sale_id"]),
                        row["sale_date"],
                        int(row["customer_id"]),
                        channel_map[row["channel"]],
                    ),
                )

            items_df = pd.read_json(io.StringIO(tables_json["sale_items"]), orient="records")
            for _, row in items_df.iterrows():
                cur.execute(
                    "INSERT INTO sale_items "
                    "(item_id, sale_id, product_id, quantity, original_price, discount_applied) "
                    "VALUES (%s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (item_id) DO NOTHING",
                    (
                        int(row["item_id"]),
                        int(row["sale_id"]),
                        int(row["product_id"]),
                        int(row["quantity"]),
                        float(row["original_price"]),
                        float(row["discount_applied"]),
                    ),
                )

            conn.commit()
            cur.close()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    csv_data = extract_from_minio()
    transformed = transform(csv_data)
    load_to_postgres(transformed)
