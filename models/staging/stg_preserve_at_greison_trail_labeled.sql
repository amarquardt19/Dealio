with source as (

    select * from {{ source('rawdatalabeling', 't12|022023|preserveatgreison') }}

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

    -- skip metadata rows (1-3) and header row (4)
    select * from ranked
    where row_num > 4

),

renamed as (

    select
        `Preserve at Greison Trail (11600)` as account_code,
        trim(_c1)                           as account_name,
        _c2                                 as mar_2022,
        _c3                                 as apr_2022,
        _c4                                 as may_2022,
        _c5                                 as jun_2022,
        _c6                                 as jul_2022,
        _c7                                 as aug_2022,
        _c8                                 as sep_2022,
        _c9                                 as oct_2022,
        _c10                                as nov_2022,
        _c11                                as dec_2022,
        _c12                                as jan_2023,
        _c13                                as feb_2023,
        _c14                                as total,
        trim(_c15)                          as proprietary_labeling,
        row_num
    from filtered

)

select * from renamed
