import pandas as pd
from datetime import date
from src.ingestion.transformer import transform_and_split


def test_filter_by_date():
    df = pd.DataFrame({
        "sale_date": ["2025-06-16", "2025-06-17", "2025-06-16"],
        "item_id": [1, 2, 3],
        "sale_id": [100, 101, 100],
        "product_id": [10, 11, 12],
        "quantity": [1, 2, 1],
        "original_price": [50.0, 30.0, 40.0],
        "unit_price": [50.0, 30.0, 40.0],
        "discount_applied": [0.0, 0.0, 0.0],
        "discount_percent": ["0.00%", "0.00%", "0.00%"],
        "discounted": [0, 0, 0],
        "item_total": [50.0, 60.0, 40.0],
        "channel": ["App Mobile", "E-commerce", "App Mobile"],
        "channel_campaigns": ["App Mobile", "Website Banner", "App Mobile"],
        "total_amount": [90.0, 60.0, 90.0],
        "product_name": ["Prod A", "Prod B", "Prod C"],
        "category": ["Shoes", "Dresses", "Shoes"],
        "brand": ["Tiva", "Tiva", "Tiva"],
        "color": ["Red", "Blue", "Red"],
        "size": ["38", "M", "40"],
        "catalog_price": [50.0, 30.0, 40.0],
        "cost_price": [25.0, 15.0, 20.0],
        "customer_id": [1, 2, 1],
        "gender": ["Female", "Female", "Female"],
        "age_range": ["26-35", "16-25", "26-35"],
        "signup_date": ["2025-01-01", "2025-02-01", "2025-01-01"],
        "first_name": ["Alice", "Bob", "Alice"],
        "last_name": ["Dupont", "Martin", "Dupont"],
        "email": ["alice@test.com", "bob@test.com", "alice@test.com"],
        "country": ["France", "Germany", "France"],
    })

    result = transform_and_split(df, date(2025, 6, 16))

    assert result is not None
    assert len(result["sale_items"]) == 2
    assert len(result["sales"]) == 1
    assert result["sales"].iloc[0]["sale_id"] == 100


def test_no_data_for_date():
    df = pd.DataFrame({
        "sale_date": ["2025-06-16"],
        "item_id": [1],
        "sale_id": [100],
        "product_id": [10],
        "quantity": [1],
        "original_price": [50.0],
        "unit_price": [50.0],
        "discount_applied": [0.0],
        "discount_percent": ["0.00%"],
        "discounted": [0],
        "item_total": [50.0],
        "channel": ["App Mobile"],
        "channel_campaigns": ["App Mobile"],
        "total_amount": [50.0],
        "product_name": ["Prod A"],
        "category": ["Shoes"],
        "brand": ["Tiva"],
        "color": ["Red"],
        "size": ["38"],
        "catalog_price": [50.0],
        "cost_price": [25.0],
        "customer_id": [1],
        "gender": ["Female"],
        "age_range": ["26-35"],
        "signup_date": ["2025-01-01"],
        "first_name": ["Alice"],
        "last_name": ["Dupont"],
        "email": ["alice@test.com"],
        "country": ["France"],
    })

    result = transform_and_split(df, date(2025, 12, 31))
    assert result is None
