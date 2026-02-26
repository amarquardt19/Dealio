-- Unified label-mismatch mart: pools mismatches from ALL properties
-- into a single dataset.  Add new properties by appending another
-- UNION ALL block below.
-- Month columns are normalized to month_01..month_12; the 'period'
-- column tells you which calendar months they represent.

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
        mar_2022 as month_01,
        apr_2022 as month_02,
        may_2022 as month_03,
        jun_2022 as month_04,
        jul_2022 as month_05,
        aug_2022 as month_06,
        sep_2022 as month_07,
        oct_2022 as month_08,
        nov_2022 as month_09,
        dec_2022 as month_10,
        jan_2023 as month_11,
        feb_2023 as month_12,
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
        sep_2024 as month_01,
        oct_2024 as month_02,
        nov_2024 as month_03,
        dec_2024 as month_04,
        jan_2025 as month_05,
        feb_2025 as month_06,
        mar_2025 as month_07,
        apr_2025 as month_08,
        may_2025 as month_09,
        jun_2025 as month_10,
        jul_2025 as month_11,
        aug_2025 as month_12,
        total
    from skytop
    where proprietary_labeling is not null

),

mismatches as (

    select *
    from normalize_map
    where lower(trim(normalized_label)) != lower(trim(section_subcategory))
      and lower(trim(normalized_label)) != 'collection loss'

)

select *
from mismatches
order by property_name, section_subcategory, proprietary_labeling
