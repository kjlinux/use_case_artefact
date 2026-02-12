import os
import psycopg2
import pandas as pd

from ..utils.logger import setup_logger

logger = setup_logger(__name__)

LOOKUP_ID_MAP = {
    "countries": "country_id",
    "categories": "category_id",
    "brands": "brand_id",
    "colors": "color_id",
    "sizes": "size_id",
    "age_ranges": "age_range_id",
}

LOOKUP_NAME_MAP = {
    "countries": "country_name",
    "categories": "category_name",
    "brands": "brand_name",
    "colors": "color_name",
    "sizes": "size_label",
    "age_ranges": "age_range_label",
}


def get_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "fashion_store"),
        user=os.getenv("PG_USER", "fashion"),
        password=os.getenv("PG_PASSWORD", "fashion123"),
    )


def upsert_lookup(cur, table, values):
    name_col = LOOKUP_NAME_MAP[table]
    id_col = LOOKUP_ID_MAP[table]

    for val in values:
        cur.execute(
            f"INSERT INTO {table} ({name_col}) VALUES (%s) ON CONFLICT ({name_col}) DO NOTHING",
            (val,),
        )

    cur.execute(f"SELECT {id_col}, {name_col} FROM {table}")
    return {row[1]: row[0] for row in cur.fetchall()}


def load_to_postgres(tables):
    conn = get_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        country_map = upsert_lookup(cur, "countries", tables["countries"]["country_name"].tolist())
        category_map = upsert_lookup(cur, "categories", tables["categories"]["category_name"].tolist())
        brand_map = upsert_lookup(cur, "brands", tables["brands"]["brand_name"].tolist())
        color_map = upsert_lookup(cur, "colors", tables["colors"]["color_name"].tolist())
        size_map = upsert_lookup(cur, "sizes", tables["sizes"]["size_label"].tolist())
        age_range_map = upsert_lookup(cur, "age_ranges", tables["age_ranges"]["age_range_label"].tolist())

        for _, row in tables["channels"].iterrows():
            cur.execute(
                "INSERT INTO channels (channel_name, campaign_name) VALUES (%s, %s) "
                "ON CONFLICT (channel_name) DO NOTHING",
                (row["channel_name"], row["campaign_name"]),
            )
        cur.execute("SELECT channel_id, channel_name FROM channels")
        channel_map = {r[1]: r[0] for r in cur.fetchall()}
        logger.info("Lookup tables loaded")

        for _, row in tables["customers"].iterrows():
            cur.execute(
                "INSERT INTO customers "
                "(customer_id, first_name, last_name, email, gender, age_range_id, signup_date, country_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (customer_id) DO UPDATE SET "
                "first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, email = EXCLUDED.email",
                (
                    int(row["customer_id"]),
                    row["first_name"] if pd.notna(row["first_name"]) else None,
                    row["last_name"] if pd.notna(row["last_name"]) else None,
                    row["email"] if pd.notna(row["email"]) else None,
                    row["gender"],
                    age_range_map[row["age_range"]],
                    row["signup_date"],
                    country_map[row["country"]],
                ),
            )
        logger.info(f"{len(tables['customers'])} customers upserted")

        for _, row in tables["products"].iterrows():
            cur.execute(
                "INSERT INTO products "
                "(product_id, product_name, category_id, brand_id, color_id, size_id, catalog_price, cost_price) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (product_id) DO NOTHING",
                (
                    int(row["product_id"]),
                    row["product_name"],
                    category_map[row["category"]],
                    brand_map[row["brand"]],
                    color_map[row["color"]],
                    size_map[str(row["size"])],
                    float(row["catalog_price"]),
                    float(row["cost_price"]),
                ),
            )
        logger.info(f"{len(tables['products'])} products upserted")

        for _, row in tables["sales"].iterrows():
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
        logger.info(f"{len(tables['sales'])} sales upserted")

        for _, row in tables["sale_items"].iterrows():
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
        logger.info(f"{len(tables['sale_items'])} sale items upserted")

        conn.commit()
        cur.close()
        logger.info("Transaction committed")

    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed, rolled back: {e}")
        raise
    finally:
        conn.close()
