with labeled as (

    select * from {{ ref('int_labeled_skytop') }}

),

-- normalize proprietary labels to match section_subcategory naming
-- so we only surface TRUE mismatches, not just abbreviation differences
normalized as (

    select
        *,
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
            else proprietary_labeling
        end as normalized_label
    from labeled
    where proprietary_labeling is not null

),

mismatches as (

    -- line items where the human-applied proprietary label
    -- differs from the section the item structurally falls under
    -- these are the most valuable training examples for a labeling model
    select
        account_code,
        account_name,
        section_category,
        section_subcategory,
        proprietary_labeling,
        normalized_label,
        sep_2024,
        oct_2024,
        nov_2024,
        dec_2024,
        jan_2025,
        feb_2025,
        mar_2025,
        apr_2025,
        may_2025,
        jun_2025,
        jul_2025,
        aug_2025,
        total,
        original_budget,
        variance,
        variance_pct,
        row_num
    from normalized
    where lower(trim(normalized_label)) != lower(trim(section_subcategory))

)

select *
from mismatches
order by section_subcategory, proprietary_labeling
