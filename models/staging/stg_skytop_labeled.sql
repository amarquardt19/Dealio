with source as (

    select * from {{ source('rawdatalabeling', 't12|0825|skytop') }}

),

add_row_index as (

    select
        *,
        monotonically_increasing_id() as _row_id
    from source

),

ranked as (

    select
        *,
        row_number() over (order by _row_id) as row_num
    from add_row_index

),

filtered as (

    -- skip metadata rows (1-3), header rows (4-6)
    select * from ranked
    where row_num > 6

),

renamed as (

    select
        `Skytop (top)`                      as account_code,
        trim(_c1)                           as account_name,
        _c2                                 as sep_2024,
        _c3                                 as oct_2024,
        _c4                                 as nov_2024,
        _c5                                 as dec_2024,
        _c6                                 as jan_2025,
        _c7                                 as feb_2025,
        _c8                                 as mar_2025,
        _c9                                 as apr_2025,
        _c10                                as may_2025,
        _c11                                as jun_2025,
        _c12                                as jul_2025,
        _c13                                as aug_2025,
        _c14                                as total,
        _c15                                as original_budget,
        _c16                                as variance,
        _c17                                as variance_pct,
        trim(_c20)                          as proprietary_label,
        row_num
    from filtered

)

select * from renamed
