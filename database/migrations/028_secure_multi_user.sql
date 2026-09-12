-- Migration 028: Multi-user security enhancements, invites and auto-lock
-- Created: 2026-04-18

BEGIN;

-- 1. Add new columns to users table
ALTER TABLE users ADD COLUMN IF NOT EXISTS permissions JSONB DEFAULT '[]'::jsonb;
ALTER TABLE users ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS totp_secret TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_totp_enabled BOOLEAN DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS failed_login_attempts INTEGER DEFAULT 0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS locked_until TIMESTAMP WITH TIME ZONE;

-- 2. Update existing admin user to have all permissions
UPDATE users 
SET permissions = '["all"]'::jsonb 
WHERE role = 'admin' AND username = 'admin';

-- 3. Create user_invites table for the onboarding system
CREATE TABLE IF NOT EXISTS user_invites (
    invite_id SERIAL PRIMARY KEY,
    token VARCHAR(64) UNIQUE NOT NULL,
    email VARCHAR(100) NOT NULL,
    permissions JSONB DEFAULT '[]'::jsonb,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    is_used BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL
);

CREATE INDEX idx_user_invites_token ON user_invites(token);

COMMENT ON TABLE user_invites IS 'Stores unique tokens for guest onboarding';

COMMIT;
