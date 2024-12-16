-- Users with Latest Preferences
SELECT u.id, u.name, p.preferred_language, p.updated_at
FROM ag_challenge_data.users u
LEFT JOIN (
    SELECT user_id, preferred_language, updated_at, 
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) AS rn
    FROM ag_challenge_data.user_preferences
) p
ON u.id = p.user_id
WHERE p.rn = 1 OR p.rn IS NULL;
