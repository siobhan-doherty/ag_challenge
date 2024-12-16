-- Combined Table Query (Users + Transactions + Preferences):
CREATE OR REPLACE TABLE ag_challenge_data.combined_table AS
SELECT 
    u.id,
    u.name,
    t.transaction_date,
    t.amount,
    p.preferred_language,
    p.updated_at AS preference_updated_at
FROM ag_challenge_data.users u
LEFT JOIN ag_challenge_data.transactions t 
ON u.id = t.user_id
LEFT JOIN ag_challenge_data.user_preferences p 
ON u.id = p.user_id;
