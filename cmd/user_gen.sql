-- CTE to insert 10 sample users
WITH user_data AS (
    SELECT
        'user' || n || '@example.com' AS email,
        'user' || n AS username,
        'User' || n AS first_name,
        'Lastname' || n AS last_name,
        -- bcrypt hash of 'password123' with cost 10
        '$2a$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi' AS password_hash,
        n % 3 != 0 AS is_active,  -- Every 3rd user is inactive
        n % 2 = 0 AS is_verified, -- Every 2nd user is verified
        CASE WHEN n % 4 = 0 THEN NULL
             ELSE (CURRENT_TIMESTAMP - (n || ' days')::INTERVAL)
        END AS last_login_at,
        (CURRENT_TIMESTAMP - ((n * 2) || ' days')::INTERVAL) AS created_at
    FROM generate_series(1, 499) AS n
)
INSERT INTO users (
    username,
    email,
    first_name,
    last_name,
    password_hash,
    is_active,
    is_verified,
    last_login_at,
    created_at,
    updated_at
)
SELECT
    username,
    email,
    first_name,
    last_name,
    password_hash,
    is_active,
    is_verified,
    last_login_at,
    created_at,
    created_at AS updated_at  -- Set updated_at same as created_at initially
FROM user_data
ON CONFLICT (email) DO NOTHING;

-- UPDATE users SET last_login_at = now() - INTERVAL '9 days';

DELETE FROM users WHERE id > 0;
