{{ config(materialized='table') }}

-- Generic labeled mart: applies section-header-based labels
-- to ALL properties processed through the generic pipeline.
-- Rental Income rows are kept but not labeled.
-- G&A uses short form 'G&A' for proprietary_labeling.

with classified as (

    select * from {{ ref('int_generic_t12') }}

)

select
    property_name,
    account_code,
    account_name,
    section_category,
    section_subcategory,
    case
        when section_subcategory = 'Rental Income' then null
        when section_subcategory = 'General & Administrative' then 'G&A'
        else section_subcategory
    end as proprietary_labeling,
    month_01, month_02, month_03, month_04,
    month_05, month_06, month_07, month_08,
    month_09, month_10, month_11, month_12,
    total,
    row_num
from classified
order by property_name, row_num
