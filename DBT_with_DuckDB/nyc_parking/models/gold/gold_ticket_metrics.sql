SELECT
    violation_code,
    count(summons_number) as ticket_count,
    sum(fee_usd) as total_revenue_usd
FROM {{ ref('silver_violation_tickets') }}
    GROUP BY violation_code
ORDER BY total_revenue_usd DESC