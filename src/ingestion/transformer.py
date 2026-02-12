import pandas as pd

from ..utils.logger import setup_logger

logger = setup_logger(__name__)


def transform_and_split(df, target_date):
    df["sale_date"] = pd.to_datetime(df["sale_date"]).dt.date

    filtered = df[df["sale_date"] == target_date].copy()
    if filtered.empty:
        return None

    logger.info(f"{len(filtered)} rows matched {target_date}")

    countries = (
        filtered[["country"]]
        .drop_duplicates()
        .rename(columns={"country": "country_name"})
    )

    categories = (
        filtered[["category"]]
        .drop_duplicates()
        .rename(columns={"category": "category_name"})
    )

    brands = (
        filtered[["brand"]]
        .drop_duplicates()
        .rename(columns={"brand": "brand_name"})
    )

    colors = (
        filtered[["color"]]
        .drop_duplicates()
        .rename(columns={"color": "color_name"})
    )

    sizes = (
        filtered[["size"]]
        .drop_duplicates()
        .rename(columns={"size": "size_label"})
    )
    sizes["size_label"] = sizes["size_label"].astype(str)

    age_ranges = (
        filtered[["age_range"]]
        .drop_duplicates()
        .rename(columns={"age_range": "age_range_label"})
    )

    channels = (
        filtered[["channel", "channel_campaigns"]]
        .drop_duplicates()
        .rename(columns={"channel": "channel_name", "channel_campaigns": "campaign_name"})
    )

    customers = filtered.drop_duplicates(subset=["customer_id"])[
        ["customer_id", "first_name", "last_name", "email", "gender",
         "age_range", "signup_date", "country"]
    ].copy()

    products = filtered.drop_duplicates(subset=["product_id"])[
        ["product_id", "product_name", "category", "brand", "color",
         "size", "catalog_price", "cost_price"]
    ].copy()
    products["size"] = products["size"].astype(str)

    sales = filtered.drop_duplicates(subset=["sale_id"])[
        ["sale_id", "sale_date", "customer_id", "channel"]
    ].copy()

    sale_items = filtered[
        ["item_id", "sale_id", "product_id", "quantity",
         "original_price", "discount_applied"]
    ].copy()

    return {
        "countries": countries,
        "categories": categories,
        "brands": brands,
        "colors": colors,
        "sizes": sizes,
        "age_ranges": age_ranges,
        "channels": channels,
        "customers": customers,
        "products": products,
        "sales": sales,
        "sale_items": sale_items,
    }
