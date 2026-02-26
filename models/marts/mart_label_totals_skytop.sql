with labeled as (

    select * from {{ ref('int_labeled_skytop') }}

),

label_totals as (

    select
        proprietary_labeling,
        count(*)   as line_item_count,
        sum(total) as annual_total
    from labeled
    where proprietary_labeling is not null
    group by proprietary_labeling

)

select *
from label_totals
order by annual_total desc
