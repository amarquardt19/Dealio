-- Unified label-mismatch mart: pools mismatches from ALL properties
-- into a single dataset.  Add new properties by appending another
-- UNION ALL block below.

with greison as (

    select * from {{ ref('int_labeled_greison_trail') }}

),

skytop as (

    select * from {{ ref('int_labeled_skytop') }}

),

-- normalize proprietary labels to match section_subcategory naming
-- so we only surface TRUE mismatches, not just abbreviation differences
normalize_map as (

    select
        'Preserve at Greison Trail' as property_name,
        'Mar 2022 - Feb 2023'       as period,
        account_code,
        account_name,
        section_category,
        section_subcategory,
        proprietary_labeling,
        case lower(trim(proprietary_labeling))
            when 'g&a'            then 'General & Administrative'
            when 'r&m'            then 'Repairs & Maintenance'
            when 'mgmt fee'       then 'Management Fees'
            when 'rubs'           then 'Utility Income'
            when 'other income'   then 'Other Income'
            when 'rental income'  then 'Rental Income'
            when 'turnover'       then 'Turnover'
            when 'payroll'        then 'Payroll'
            when 'utilities'      then 'Utilities'
            when 'insurance'      then 'Insurance'
            when 'marketing'      then 'Marketing'
            when 'taxes'          then 'Taxes'
            when 'contract'       then 'Contract'
            when 'collection loss' then 'Collection Loss'
            else proprietary_labeling
        end as normalized_label,
        total
    from greison
    where proprietary_labeling is not null

    union all

    select
        'Skytop'                    as property_name,
        'Sep 2024 - Aug 2025'       as period,
        account_code,
        account_name,
        section_category,
        section_subcategory,
        proprietary_labeling,
        case lower(trim(proprietary_labeling))
            when 'g&a'            then 'General & Administrative'
            when 'r&m'            then 'Repairs & Maintenance'
            when 'mgmt fee'       then 'Management Fees'
            when 'rubs'           then 'Utility Income'
            when 'other income'   then 'Other Income'
            when 'rental income'  then 'Rental Income'
            when 'turnover'       then 'Turnover'
            when 'payroll'        then 'Payroll'
            when 'utilities'      then 'Utilities'
            when 'insurance'      then 'Insurance'
            when 'marketing'      then 'Marketing'
            when 'taxes'          then 'Taxes'
            when 'contract'       then 'Contract'
            when 'collection loss' then 'Collection Loss'
            else proprietary_labeling
        end as normalized_label,
        total
    from skytop
    where proprietary_labeling is not null

),

mismatches as (

    select *
    from normalize_map
    where lower(trim(normalized_label)) != lower(trim(section_subcategory))

)

select *
from mismatches
order by property_name, section_subcategory, proprietary_labeling
