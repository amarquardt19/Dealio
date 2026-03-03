{{ config(materialized='table') }}

-- Unified labeled mart: ALL properties, ALL line items, one table.
-- Combines legacy property-specific models + generic pipeline.

with greison as (

    select
        'Preserve at Greison Trail' as property_name,
        account_code,
        account_name,
        section_category,
        section_subcategory,
        proprietary_labeling,
        mar_2022 as month_01, apr_2022 as month_02, may_2022 as month_03,
        jun_2022 as month_04, jul_2022 as month_05, aug_2022 as month_06,
        sep_2022 as month_07, oct_2022 as month_08, nov_2022 as month_09,
        dec_2022 as month_10, jan_2023 as month_11, feb_2023 as month_12,
        total
    from {{ ref('int_labeled_greison_trail') }}

),

skytop as (

    select
        'Skytop' as property_name,
        account_code,
        account_name,
        section_category,
        section_subcategory,
        proprietary_labeling,
        sep_2024 as month_01, oct_2024 as month_02, nov_2024 as month_03,
        dec_2024 as month_04, jan_2025 as month_05, feb_2025 as month_06,
        mar_2025 as month_07, apr_2025 as month_08, may_2025 as month_09,
        jun_2025 as month_10, jul_2025 as month_11, aug_2025 as month_12,
        total
    from {{ ref('int_labeled_skytop') }}

),

generic as (

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
        total
    from {{ ref('mart_generic_labeled') }}

)

select * from greison
union all
select * from skytop
union all
select * from generic
