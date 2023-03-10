{% macro price_volume_mix_analysis (quantity_col, price_col, product_col) %}

WITH revenue_by_product AS (
  SELECT 
    {{ product_col }},
    SUM({{ quantity_col }} * {{ price_col }}) AS revenue
  FROM {{ source('your_source_name', 'sales_data') }}
  GROUP BY 1
),
total_revenue AS (
  SELECT
    SUM(revenue) AS total_revenue
  FROM revenue_by_product
),
volume_by_product AS (
  SELECT 
    {{ product_col }},
    SUM({{ quantity_col }}) AS volume
  FROM {{ source('your_source_name', 'sales_data') }}
  GROUP BY 1
),
total_volume AS (
  SELECT
    SUM(volume) AS total_volume
  FROM volume_by_product
),
price_by_product AS (
  SELECT 
    {{ product_col }},
    AVG({{ price_col }}) AS price
  FROM {{ source('your_source_name', 'sales_data') }}
  GROUP BY 1
),
average_price AS (
  SELECT
    SUM(price * volume) / SUM(volume) AS average_price
  FROM price_by_product
  JOIN volume_by_product USING ({{ product_col }})
),
mix_by_product AS (
  SELECT 
    {{ product_col }},
    SUM({{ quantity_col }}) / total_volume AS mix
  FROM {{ source('your_source_name', 'sales_data') }}
  CROSS JOIN total_volume
  GROUP BY 1
),
price_effect AS (
  SELECT
    SUM(mix * (price - average_price) * total_volume) / total_revenue AS price_effect
  FROM mix_by_product
  JOIN price_by_product USING ({{ product_col }})
  CROSS JOIN total_revenue
),
volume_effect AS (
  SELECT
    SUM((mix - 1) * price * volume) / total_revenue AS volume_effect
  FROM mix_by_product
  JOIN price_by_product USING ({{ product_col }})
  JOIN volume_by_product USING ({{ product_col }})
  CROSS JOIN total_revenue
),
mix_effect AS (
  SELECT
    SUM((mix - 1) * price * volume) / total_revenue AS mix_effect
  FROM mix_by_product
  JOIN price_by_product USING ({{ product_col }})
  JOIN volume_by_product USING ({{ product_col }})
  CROSS JOIN total_revenue
)

SELECT
  *,
  price_effect + volume_effect + mix_effect AS total_effect
FROM (
  SELECT
    {{ product_col }},
    revenue,
    volume,
    price,
    mix,
    mix * (price - average_price) * total_volume / total_revenue AS price_effect,
    (mix - 1) * price * volume / total_revenue AS volume_effect,
    (mix - 1) * price * volume / total_revenue AS mix_effect
  FROM revenue_by_product
  JOIN volume_by_product USING ({{ product_col }})
  JOIN price_by_product USING ({{ product_col }})
  JOIN mix_by_product USING ({{ product_col }})
  CROSS JOIN total_revenue
  CROSS JOIN total_volume
  CROSS JOIN average_price
) AS analysis

{% endmacro %}
