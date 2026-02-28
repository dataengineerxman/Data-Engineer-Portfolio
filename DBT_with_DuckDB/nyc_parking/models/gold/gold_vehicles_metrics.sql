SELECT
    registration_state,
    count(summons_number) as ticket_count,
FROM {{ ref('silver_violation_tickets') }}
WHERE registration_state!='NY'
    GROUP BY registration_state
ORDER BY ticket_count DESC