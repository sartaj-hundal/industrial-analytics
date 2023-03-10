-- Description: Sources fleet vehicle rental data and performs some typical analyses
-- Inputs:
--   - rental_data: a table containing fleet vehicle rental data
-- Outputs:
--   - rental_analytics: a table containing rental data with some typical analyses

-- Load the rental data
{{ source('your_source_name', 'rental_data') }}

-- Define some macros for analyses
{% macro rental_duration(start_date, end_date) %}
  CASE 
    WHEN start_date IS NULL OR end_date IS NULL THEN NULL
    ELSE end_date - start_date
  END
{% endmacro %}

{% macro rental_revenue(rental_duration, daily_rate) %}
  CASE 
    WHEN rental_duration IS NULL OR daily_rate IS NULL OR rental_duration = 0 THEN NULL
    ELSE rental_duration * daily_rate
  END
{% endmacro %}

{% macro rental_profit(rental_revenue, cost_of_rental) %}
  CASE 
    WHEN rental_revenue IS NULL OR cost_of_rental IS NULL THEN NULL
    ELSE rental_revenue - cost_of_rental
  END
{% endmacro %}

-- Perform some typical analyses on the rental data
{{ 
  config(
    materialized='table',
    unique_key='id',
    alias='rental_analytics'
  )
}}
SELECT
  *,
  {{ rental_duration('start_date', 'end_date') }} AS rental_duration,
  {{ rental_revenue('rental_duration', 'daily_rate') }} AS rental_revenue,
  {{ rental_profit('rental_revenue', 'cost_of_rental') }} AS rental_profit
FROM {{ ref('rental_data') }}
