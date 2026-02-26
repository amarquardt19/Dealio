with labeled as (

    select * from {{ ref('int_labeled_greison_trail') }}

),

label_totals as (

    select
        proprietary_labeling,
        count(*)                            as line_item_count,
        sum(total)                          as annual_total,
        sum(mar_2022)                       as mar_2022,
        sum(apr_2022)                       as apr_2022,
        sum(may_2022)                       as may_2022,
        sum(jun_2022)                       as jun_2022,
        sum(jul_2022)                       as jul_2022,
        sum(aug_2022)                       as aug_2022,
        sum(sep_2022)                       as sep_2022,
        sum(oct_2022)                       as oct_2022,
        sum(nov_2022)                       as nov_2022,
        sum(dec_2022)                       as dec_2022,
        sum(jan_2023)                       as jan_2023,
        sum(feb_2023)                       as feb_2023
    from labeled
    where proprietary_labeling is not null
    group by proprietary_labeling

)

select *
from label_totals
order by annual_total desc
