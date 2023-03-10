-- Description: Sources financial data from Oracle EPM Cloud and performs some typical financial calculations
-- Inputs:
--   - epm_data: a table containing financial data from Oracle EPM Cloud
-- Outputs:
--   - finance_data: a table containing financial data with some typical financial calculations

-- Load the financial data from Oracle EPM Cloud
{{ source('your_source_name', 'epm_data') }}

-- Define some macros for financial calculations
{% macro gross_margin(revenue, cost_of_goods_sold) %}
  CASE 
    WHEN revenue IS NULL OR cost_of_goods_sold IS NULL OR revenue = 0 THEN NULL
    ELSE (revenue - cost_of_goods_sold) / revenue
  END
{% endmacro %}

{% macro net_profit_margin(profit, revenue) %}
  CASE 
    WHEN profit IS NULL OR revenue IS NULL OR revenue = 0 THEN NULL
    ELSE profit / revenue
  END
{% endmacro %}

-- Perform some typical financial calculations on the financial data
{{ 
  config(
    materialized='table',
    unique_key='id',
    alias='finance_data'
  )
}}
SELECT
  *,
  {{ gross_margin('revenue', 'cost_of_goods_sold') }} AS gross_margin,
  {{ net_profit_margin('net_income', 'revenue') }} AS net_profit_margin
FROM {{ ref('epm_data') }}
