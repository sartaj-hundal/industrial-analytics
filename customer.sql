-- Description: Sources customer and contract-level data, as well as organizational hierarchy, and performs some simple SELECT statements
-- Inputs:
--   - customer_data: a table containing customer-level data
--   - contract_data: a table containing contract-level data
--   - org_hierarchy: a table containing organizational hierarchy for each segment
-- Outputs:
--   - customer_analytics: a table containing customer data with some simple SELECT statements

-- Load the customer and contract data
{{ source('your_source_name', 'customer_data') }}
{{ source('your_source_name', 'contract_data') }}

-- Load the organizational hierarchy data
{{ source('your_source_name', 'org_hierarchy') }}

-- Perform some simple SELECT statements on the customer data
{{ 
  config(
    materialized='table',
    unique_key='customer_id',
    alias='customer_analytics'
  )
}}
SELECT
  customer_data.customer_id,
  customer_data.customer_name,
  contract_data.contract_id,
  contract_data.contract_start_date,
  contract_data.contract_end_date,
  org_hierarchy.ceo_name,
  org_hierarchy.group_finance_manager_name,
  org_hierarchy.customer_center_manager_name
FROM {{ ref('customer_data') }} AS customer_data
LEFT JOIN {{ ref('contract_data') }} AS contract_data ON customer_data.customer_id = contract_data.customer_id
LEFT JOIN {{ ref('org_hierarchy') }} AS org_hierarchy ON customer_data.segment = org_hierarchy.segment
