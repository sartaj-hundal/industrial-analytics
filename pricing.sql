-- Description: Sources pricing data from all segments and joins it with customer and contract data to enable reporting and analysis
-- Inputs:
--   - pricing_data: a table containing pricing data from all segments
--   - customer_data: a table containing customer data, including customer IDs
--   - contract_data: a table containing contract data, including contract IDs and customer IDs
-- Outputs:
--   - pricing_with_customer_and_contract: a table containing pricing data joined with customer and contract data

-- Load the pricing data
{{ source('your_source_name', 'pricing_data') }}

-- Load the customer data

{{ source('your_source_name', 'customer_data') }}
-- Load the contract data
{{ source('your_source_name', 'contract_data') }}

-- Join the pricing data with the customer data using the customer ID
{{ 
  config(
    materialized='view',
    unique_key='id',
    alias='pricing_with_customer'
  )
}}
SELECT
  p.*,
  c.customer_name,
  c.customer_segment
FROM {{ ref('pricing_data') }} p
JOIN {{ ref('customer_data') }} c
ON p.customer_id = c.customer_id

-- Join the pricing with customer data with the contract data using the customer ID and contract ID
{{ 
  config(
    materialized='table',
    unique_key='id',
    alias='pricing_with_customer_and_contract'
  )
}}
SELECT
  p.*,
  c.contract_id
FROM {{ ref('pricing_with_customer') }} p
JOIN {{ ref('contract_data') }} c
ON p.customer_id = c.customer_id
AND p.contract_id = c.contract_id
