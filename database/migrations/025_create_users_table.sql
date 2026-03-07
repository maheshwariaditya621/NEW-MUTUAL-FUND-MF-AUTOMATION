-- Migration 025: Create Users Table for Authentication
-- Created: 2026-03-04

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) DEFAULT 'user',
    is_active BOOLEAN DEFAULT TRUE,
    last_login TIMESTAMP
    WITH
        TIME ZONE,
        created_at TIMESTAMP
    WITH
        TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_username ON users (username);

-- Seed initial admin user
-- Password: admin123 (Generated using bcrypt)
INSERT INTO
    users (
        username,
        email,
        password_hash,
        role
    )
VALUES (
        'admin',
        'admin@mf-analytics.com',
        '$2b$12$GfTPdmQieHZByJVCsx2PR.xuDLrt62hVByqyDfkaC65kAO1BDxf7G',
        'admin'
    );