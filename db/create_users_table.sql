-- Create users table for battery authentication
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    battery_id VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    first_login BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_login TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT TRUE
);

-- Create index for faster battery_id lookups
CREATE INDEX IF NOT EXISTS users_battery_id_idx ON users (battery_id);

-- Create index for active users
CREATE INDEX IF NOT EXISTS users_active_idx ON users (is_active) WHERE is_active = TRUE;
