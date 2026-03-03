-- AI-powered label review: runs ai_query() on EVERY labeled line item
-- across ALL properties from the generic pipeline.
-- Provides CORRECT_LABEL / CONFIDENCE / REASON alongside rule-based labels.
-- Uses verified precedents from Greison Trail & Skytop as few-shot examples.

{{ config(materialized='table') }}

with labeled as (

    select
        property_name,
        account_code,
        account_name,
        section_category,
        section_subcategory,
        proprietary_labeling,
        month_01, month_02, month_03, month_04,
        month_05, month_06, month_07, month_08,
        month_09, month_10, month_11, month_12,
        total,
        row_num
    from {{ ref('mart_generic_labeled') }}

)

select
    property_name,
    account_code,
    account_name,
    section_category,
    section_subcategory,
    proprietary_labeling,
    month_01, month_02, month_03, month_04,
    month_05, month_06, month_07, month_08,
    month_09, month_10, month_11, month_12,
    total,
    ai_query(
        'databricks-meta-llama-3-3-70b-instruct',
        concat(
            'You are an expert at classifying line items on multifamily apartment operating statements (T12s). ',
            'Review this line item and confirm or correct its classification.\n\n',

            'ALLOWED LABELS (use ONLY one of these):\n',
            'Rental Income, Collection Loss, Other Income, RUBS, Payroll, ',
            'Repairs & Maintenance, Turnover, Contract, Marketing, G&A, ',
            'Utilities, Management Fees, Taxes, Insurance\n\n',

            'RULES:\n',
            '- RUBS = resident utility billing/reimbursements (electric, gas, trash, water/sewer rebills or reimbursements). NOT pest control.\n',
            '- Collection Loss = vacancy, concessions, bad debt, lost rent, write-offs\n',
            '- Trash Removal in expense sections → Utilities\n',
            '- Pest Control / Exterminating → Contract\n',
            '- Property Insurance / Liability Insurance → Insurance. Renters Insurance → G&A\n',
            '- Property Taxes → Taxes\n',
            '- G&A = general & administrative (office, legal, accounting, technology, employee costs)\n',
            '- Trust the section the item came from unless there is a clear reason to reclassify\n\n',

            'VERIFIED PRECEDENTS from other properties:\n',
            '- Electric Rebill / Electric Reimbursement → RUBS\n',
            '- Gas Rebill / Gas Reimbursement → RUBS\n',
            '- Trash Rebill / Trash Reimbursement → RUBS\n',
            '- Water/Sewer Rebill / Water/Sewer Reimbursement → RUBS\n',
            '- Utility Income → RUBS\n',
            '- Pest Control Reimbursement → Other Income (NOT RUBS)\n',
            '- Door to Door Trash Fee → Other Income\n',
            '- Trash Removal (in expenses) → Utilities\n',
            '- Exterminating / Pest Control → Contract\n',
            '- Vacancy → Collection Loss\n',
            '- Concessions / Lost Rent → Collection Loss\n',
            '- Bad Debt Recovery → Collection Loss\n',
            '- Renters Insurance Policy → G&A\n',
            '- Property Insurance → Insurance\n',
            '- Property Taxes → Taxes\n',
            '- Management Fees → Management Fees\n',
            '- Landscaping → Contract\n',
            '- Turnkey / Turn Painting / Turn Cleaning → Turnover\n\n',

            'LINE ITEM TO CLASSIFY:\n',
            '- Account Name: ', account_name, '\n',
            '- Current Section: ', section_subcategory, '\n',
            '- Annual Total: $', cast(round(coalesce(total, 0), 2) as string), '\n\n',
            'Respond in EXACTLY this format (3 lines, nothing else):\n',
            'CORRECT_LABEL: [label]\n',
            'CONFIDENCE: [high/medium/low]\n',
            'REASON: [one sentence]'
        )
    ) as ai_response
from labeled
order by property_name, row_num
