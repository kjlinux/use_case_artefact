CREATE OR REPLACE VIEW v_star_schema AS
SELECT
    si.item_id,
    s.sale_id,
    s.sale_date,

    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.gender,
    ar.age_range_label AS age_range,
    c.signup_date,
    co.country_name AS country,

    p.product_id,
    p.product_name,
    cat.category_name AS category,
    b.brand_name AS brand,
    col.color_name AS color,
    sz.size_label AS size,
    p.catalog_price,
    p.cost_price,

    ch.channel_name AS channel,
    ch.campaign_name AS channel_campaigns,

    si.quantity,
    si.original_price,
    si.discount_applied,
    CASE WHEN si.discount_applied > 0 THEN 1 ELSE 0 END AS discounted,
    ROUND(si.discount_applied / NULLIF(si.original_price, 0), 4) AS discount_percent,
    (si.original_price - si.discount_applied) AS unit_price,
    (si.quantity * (si.original_price - si.discount_applied)) AS item_total
FROM sale_items si
JOIN sales s ON si.sale_id = s.sale_id
JOIN customers c ON s.customer_id = c.customer_id
JOIN products p ON si.product_id = p.product_id
JOIN channels ch ON s.channel_id = ch.channel_id
JOIN countries co ON c.country_id = co.country_id
JOIN age_ranges ar ON c.age_range_id = ar.age_range_id
JOIN categories cat ON p.category_id = cat.category_id
JOIN brands b ON p.brand_id = b.brand_id
JOIN colors col ON p.color_id = col.color_id
JOIN sizes sz ON p.size_id = sz.size_id;

CREATE OR REPLACE VIEW v_sale_totals AS
SELECT
    sale_id,
    SUM(quantity * (original_price - discount_applied)) AS total_amount
FROM sale_items
GROUP BY sale_id;
