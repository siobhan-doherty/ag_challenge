-- Transaction Summary by Date and User
SELECT 
    t.user_id,
    DATE(t.transaction_date) AS transaction_date,
    SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) AS total_deposits,
    SUM(CASE WHEN t.amount < 0 THEN t.amount ELSE 0 END) AS total_withdrawals
FROM ag_challenge_data.transactions t
GROUP BY t.user_id, transaction_date
ORDER BY transaction_date, t.user_id;
