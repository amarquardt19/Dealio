{{ config(materialized='table') }}

select
    property_name,
    account_code,
    account_name,
    section_category,
    section_subcategory,
    proprietary_labeling,
    month_01, month_02, month_03, month_04,
    month_05, month_06, month_07, month_08,
    month_09, month_10, month_11, month_12,
    total,
    ai_response
from {{ ref('mart_ai_labeled') }}
where property_name = 'eastponcevillage'
