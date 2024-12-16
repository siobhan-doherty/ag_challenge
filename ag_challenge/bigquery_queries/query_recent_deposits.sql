-- Recent Deposits (last 30 days)
SELECT DISTINCT 
    u.id AS user_id, 
    u.name, 
    t.transaction_date, 
    t.amount
FROM ag_challenge_data.users u
JOIN ag_challenge_data.transactions t
ON u.id = t.user_id
WHERE t.amount > 0  -- Deposits are positive values
    AND t.transaction_date >= DATE(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY));
