{{
    config(
        materialized='table'
    )
}}

with source as (

    select * from {{ ref('int_preserve_at_greison_trail') }}

),

unpivoted as (

    select category, subcategory, account_code, account_name, cast('2022-03-01' as date) as period_date, mar_2022 as amount from source
    union all
    select category, subcategory, account_code, account_name, cast('2022-04-01' as date) as period_date, apr_2022 as amount from source
    union all
    select category, subcategory, account_code, account_name, cast('2022-05-01' as date) as period_date, may_2022 as amount from source
    union all
    select category, subcategory, account_code, account_name, cast('2022-06-01' as date) as period_date, jun_2022 as amount from source
    union all
    select category, subcategory, account_code, account_name, cast('2022-07-01' as date) as period_date, jul_2022 as amount from source
    union all
    select category, subcategory, account_code, account_name, cast('2022-08-01' as date) as period_date, aug_2022 as amount from source
    union all
    select category, subcategory, account_code, account_name, cast('2022-09-01' as date) as period_date, sep_2022 as amount from source
    union all
    select category, subcategory, account_code, account_name, cast('2022-10-01' as date) as period_date, oct_2022 as amount from source
    union all
    select category, subcategory, account_code, account_name, cast('2022-11-01' as date) as period_date, nov_2022 as amount from source
    union all
    select category, subcategory, account_code, account_name, cast('2022-12-01' as date) as period_date, dec_2022 as amount from source
    union all
    select category, subcategory, account_code, account_name, cast('2023-01-01' as date) as period_date, jan_2023 as amount from source
    union all
    select category, subcategory, account_code, account_name, cast('2023-02-01' as date) as period_date, feb_2023 as amount from source

)

select
    category,
    subcategory,
    account_code,
    account_name,
    period_date,
    coalesce(amount, 0) as amount
from unpivoted
order by account_code, period_date
