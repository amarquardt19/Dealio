with labeled as (

    select * from {{ ref('stg_skytop_labeled') }}

),

line_items_only as (

    -- remove section headers (all monthly values are null) and any total/net/subtotal rows
    select *
    from labeled
    where
        not (
            sep_2024 is null
            and oct_2024 is null
            and nov_2024 is null
            and dec_2024 is null
            and jan_2025 is null
            and feb_2025 is null
            and mar_2025 is null
            and apr_2025 is null
            and may_2025 is null
            and jun_2025 is null
            and jul_2025 is null
            and aug_2025 is null
            and total is null
        )
        and upper(account_name) not like '%TOTAL%'
        and upper(account_name) not like 'NET %'

),

classified as (

    select
        case
            when account_code < '5000' then 'Revenue'
            else 'Expenses'
        end as section_category,

        case
            -- Revenue subcategories
            when account_code between '4030-000' and '4060-999' then 'Rental Income'
            when account_code between '4100-000' and '4199-999' then 'Rental Income'
            when account_code between '4200-000' and '4399-999' then 'Utility Income'
            when account_code between '4950-000' and '4950-999' then 'Utility Income'
            when account_code between '4400-000' and '4999-999' then 'Other Income'
            -- Expense subcategories
            when account_code between '5020-000' and '5115-999' then 'Payroll'
            when account_code between '5120-000' and '5234-999' then 'General & Administrative'
            when account_code between '5235-000' and '5235-999' then 'Management Fees'
            when account_code between '5236-000' and '5270-999' then 'General & Administrative'
            when account_code between '5310-000' and '5365-999' then 'Repairs & Maintenance'
            when account_code between '5410-000' and '5475-999' then 'Marketing'
            when account_code between '5615-000' and '5850-999' then 'Repairs & Maintenance'
            when account_code between '5910-000' and '5940-999' then 'Turnover'
            when account_code between '6410-000' and '6559-999' then 'Utilities'
            when account_code between '6560-000' and '6599-999' then 'Insurance'
            else 'Other'
        end as section_subcategory,

        account_code,
        account_name,
        proprietary_labeling,

        cast(replace(sep_2024, ',', '') as double) as sep_2024,
        cast(replace(oct_2024, ',', '') as double) as oct_2024,
        cast(replace(nov_2024, ',', '') as double) as nov_2024,
        cast(replace(dec_2024, ',', '') as double) as dec_2024,
        cast(replace(jan_2025, ',', '') as double) as jan_2025,
        cast(replace(feb_2025, ',', '') as double) as feb_2025,
        cast(replace(mar_2025, ',', '') as double) as mar_2025,
        cast(replace(apr_2025, ',', '') as double) as apr_2025,
        cast(replace(may_2025, ',', '') as double) as may_2025,
        cast(replace(jun_2025, ',', '') as double) as jun_2025,
        cast(replace(jul_2025, ',', '') as double) as jul_2025,
        cast(replace(aug_2025, ',', '') as double) as aug_2025,
        cast(replace(total, ',', '') as double)    as total,
        cast(replace(original_budget, ',', '') as double) as original_budget,
        cast(replace(variance, ',', '') as double) as variance,
        variance_pct,
        row_num

    from line_items_only

)

select * from classified
