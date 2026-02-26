with labeled_1 as (

    select * from {{ ref('stg_preserve_at_greison_trail_labeled') }}

),

labeled_2 as (

    select * from {{ ref('stg_preserve_at_greison_trail_labeled_2') }}

),

unioned as (

    select *, 'preserveatgreisontrail' as source_doc from labeled_1
    union all
    select *, 'preserveatgreisontrail2' as source_doc from labeled_2

),

line_items_only as (

    -- remove section headers (all monthly values are null) and any total/net/subtotal rows
    select *
    from unioned
    where
        not (
            mar_2022 is null
            and apr_2022 is null
            and may_2022 is null
            and jun_2022 is null
            and jul_2022 is null
            and aug_2022 is null
            and sep_2022 is null
            and oct_2022 is null
            and nov_2022 is null
            and dec_2022 is null
            and jan_2023 is null
            and feb_2023 is null
            and total is null
        )
        and upper(account_name) not like '%TOTAL%'
        and upper(account_name) not like 'NET %'

),

classified as (

    select
        case
            when account_code < '5000' then 'Revenue'
            when account_code < '6500' then 'Expenses'
        end as section_category,

        case
            -- Revenue subcategories
            when account_code between '4000-0000' and '4060-0000' then 'Rental Income'
            when account_code between '4081-0000' and '4081-9999' then 'Utility Income'
            when account_code between '4400-0000' and '4699-0000' then 'Other Income'
            -- Expense subcategories
            when account_code between '5020-0000' and '5049-9999' then 'Payroll'
            when account_code between '5056-0000' and '5056-9999' then 'General & Administrative'
            when account_code between '5095-0000' and '5108-0000' then 'Marketing'
            when account_code between '5200-0000' and '5215-0000' then 'Repairs & Maintenance'
            when account_code between '5299-0000' and '5399-0000' then 'Utilities'
            when account_code between '5460-0000' and '5465-0000' then 'Management Fees'
            when account_code between '5499-0000' and '5515-0000' then 'Taxes'
            when account_code between '5519-0000' and '5520-9999' then 'Insurance'
            else 'Other'
        end as section_subcategory,

        account_code,
        account_name,
        proprietary_labeling,

        cast(replace(mar_2022, ',', '') as double) as mar_2022,
        cast(replace(apr_2022, ',', '') as double) as apr_2022,
        cast(replace(may_2022, ',', '') as double) as may_2022,
        cast(replace(jun_2022, ',', '') as double) as jun_2022,
        cast(replace(jul_2022, ',', '') as double) as jul_2022,
        cast(replace(aug_2022, ',', '') as double) as aug_2022,
        cast(replace(sep_2022, ',', '') as double) as sep_2022,
        cast(replace(oct_2022, ',', '') as double) as oct_2022,
        cast(replace(nov_2022, ',', '') as double) as nov_2022,
        cast(replace(dec_2022, ',', '') as double) as dec_2022,
        cast(replace(jan_2023, ',', '') as double) as jan_2023,
        cast(replace(feb_2023, ',', '') as double) as feb_2023,
        cast(replace(total, ',', '') as double)    as total,
        source_doc,
        row_num

    from line_items_only

)

select * from classified
