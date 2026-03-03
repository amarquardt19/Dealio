-- Generic T12 intermediate: detects section headers from the raw document
-- structure and propagates them down to classify every line item.
-- Works for ANY property regardless of account code format.

with staged as (

    select * from {{ ref('stg_generic_t12') }}

),

-- Step 1: Detect section headers — rows with a recognized section name
-- and no financial data (account_name matches a known header).
with_headers as (

    select
        *,
        case
            when trim(account_name) in (
                'Rental Income',
                'Vacancy, Losses & Concessions',
                'Vacancy, Loss & Concessions',
                'Vacancy Loss & Concessions',
                'Concessions',
                'Lost Rent',
                'Other Income',
                'Payroll & Related',
                'Payroll',
                'Maintenance & Repairs',
                'Repairs & Maintenance',
                'Turnover Expense',
                'Turnover',
                'Contract Services',
                'Contract',
                'Marketing Expenses',
                'Marketing',
                'Administrative Expenses',
                'General & Administrative',
                'Utilities',
                'Management Fees',
                'Management Fee',
                'Taxes & Insurance',
                'Taxes',
                'Insurance',
                'Non-Revenue Income',
                'Bad Debt',
                'Turnkey',
                'Management & Professional Fees',
                'Property Taxes',
                'Interest & Misc Expense',
                'Capital Expenses',
                'Operating Expenses'
            )
            -- Only treat as section header if the row has NO financial data
            and nullif(trim(coalesce(month_01, '')), '') is null
            then trim(account_name)
            else null
        end as section_header
    from staged

),

-- Step 2: Propagate the most recent section header down per property.
propagated as (

    select
        *,
        last_value(section_header, true) over (
            partition by property_name
            order by row_num
            rows between unbounded preceding and current row
        ) as current_section
    from with_headers

),

-- Step 3: Keep only actual line items.
-- A row is a data row if it's under a known section, is not itself a header,
-- is not a total/net row, and has a non-empty account name.
line_items_only as (

    select *
    from propagated
    where current_section is not null
      and section_header is null
      and trim(coalesce(account_name, '')) != ''
      and upper(account_name) not like '%TOTAL%'
      and upper(account_name) not like 'NET %'
      and upper(account_name) not like '%SUBTOTAL%'
      and upper(account_name) != 'INCOME'
      and upper(account_name) != 'EXPENSES'
      and upper(account_name) != 'CONTROLLABLE EXPENSES'
      and upper(account_name) != 'NON-CONTROLLABLE EXPENSES'
      and upper(account_name) != 'ACTUAL'
      and account_name not rlike '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
      and upper(account_name) != 'MONTH ENDING'
      and upper(account_name) != 'OPERATING EXPENSES'
      -- Exclude below-NOI sections
      and current_section not in ('Interest & Misc Expense', 'Capital Expenses')

),

-- Step 4: Map section headers to standard categories + subcategories.
classified as (

    select
        property_name,

        case
            when current_section in (
                'Rental Income', 'Vacancy, Losses & Concessions',
                'Vacancy, Loss & Concessions', 'Vacancy Loss & Concessions',
                'Concessions', 'Lost Rent',
                'Other Income', 'Non-Revenue Income'
            ) then 'Revenue'
            else 'Expenses'
        end as section_category,

        case
            when current_section = 'Rental Income'
                then 'Rental Income'
            when current_section in ('Vacancy, Losses & Concessions',
                                     'Vacancy, Loss & Concessions',
                                     'Vacancy Loss & Concessions',
                                     'Concessions', 'Lost Rent')
                then 'Collection Loss'
            -- Standalone Vacancy line items (has data, not a section header)
            when current_section = 'Rental Income'
                 and lower(trim(account_name)) = 'vacancy'
                then 'Collection Loss'
            -- RUBS: utility rebills/reimbursements within Other Income
            -- NOT pest control, NOT legal fee rebills
            when current_section = 'Other Income'
                 and (lower(account_name) like '%electric rebill%'
                      or lower(account_name) like '%electric reimbursement%'
                      or lower(account_name) like '%trash rebill%'
                      or lower(account_name) like '%trash reimbursement%'
                      or lower(account_name) like '%gas rebill%'
                      or lower(account_name) like '%gas reimbursement%'
                      or lower(account_name) like '%water/sewer rebill%'
                      or lower(account_name) like '%water/sewer reimbursement%'
                      or lower(account_name) like '%water sewer rebill%'
                      or lower(account_name) like '%water sewer reimbursement%'
                      or lower(account_name) like '%water / sewer reimbursement%'
                      or lower(account_name) = 'utility income')
                 and lower(account_name) not like '%pest control%'
                then 'RUBS'
            when current_section in ('Other Income', 'Non-Revenue Income')
                then 'Other Income'
            when current_section in ('Payroll & Related', 'Payroll')
                then 'Payroll'
            when current_section in ('Maintenance & Repairs', 'Repairs & Maintenance')
                then 'Repairs & Maintenance'
            when current_section in ('Turnover Expense', 'Turnover', 'Turnkey')
                then 'Turnover'
            -- Trash Removal in Contract → Utilities
            when current_section in ('Contract Services', 'Contract')
                 and lower(account_name) like '%trash removal%'
                then 'Utilities'
            when current_section in ('Contract Services', 'Contract')
                then 'Contract'
            when current_section in ('Marketing Expenses', 'Marketing')
                then 'Marketing'
            when current_section in ('Administrative Expenses', 'General & Administrative')
                then 'General & Administrative'
            when current_section = 'Utilities'
                then 'Utilities'
            when current_section in ('Management Fees', 'Management Fee', 'Management & Professional Fees')
                then 'Management Fees'
            when current_section = 'Taxes & Insurance'
                 and lower(account_name) like '%insurance%'
                then 'Insurance'
            when current_section in ('Taxes & Insurance', 'Taxes', 'Property Taxes')
                then 'Taxes'
            when current_section = 'Insurance'
                then 'Insurance'
            when current_section = 'Bad Debt'
                then 'Collection Loss'
            else 'Other'
        end as section_subcategory,

        account_code,
        account_name,

        try_cast(replace(replace(replace(nullif(trim(month_01), ''), ',', ''), '(', '-'), ')', '') as double) as month_01,
        try_cast(replace(replace(replace(nullif(trim(month_02), ''), ',', ''), '(', '-'), ')', '') as double) as month_02,
        try_cast(replace(replace(replace(nullif(trim(month_03), ''), ',', ''), '(', '-'), ')', '') as double) as month_03,
        try_cast(replace(replace(replace(nullif(trim(month_04), ''), ',', ''), '(', '-'), ')', '') as double) as month_04,
        try_cast(replace(replace(replace(nullif(trim(month_05), ''), ',', ''), '(', '-'), ')', '') as double) as month_05,
        try_cast(replace(replace(replace(nullif(trim(month_06), ''), ',', ''), '(', '-'), ')', '') as double) as month_06,
        try_cast(replace(replace(replace(nullif(trim(month_07), ''), ',', ''), '(', '-'), ')', '') as double) as month_07,
        try_cast(replace(replace(replace(nullif(trim(month_08), ''), ',', ''), '(', '-'), ')', '') as double) as month_08,
        try_cast(replace(replace(replace(nullif(trim(month_09), ''), ',', ''), '(', '-'), ')', '') as double) as month_09,
        try_cast(replace(replace(replace(nullif(trim(month_10), ''), ',', ''), '(', '-'), ')', '') as double) as month_10,
        try_cast(replace(replace(replace(nullif(trim(month_11), ''), ',', ''), '(', '-'), ')', '') as double) as month_11,
        try_cast(replace(replace(replace(nullif(trim(month_12), ''), ',', ''), '(', '-'), ')', '') as double) as month_12,
        try_cast(replace(replace(replace(nullif(trim(total), ''), ',', ''), '(', '-'), ')', '') as double)    as total,
        row_num

    from line_items_only

)

select * from classified
