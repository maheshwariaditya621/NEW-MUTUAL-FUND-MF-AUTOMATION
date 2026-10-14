import psycopg2
import json
from src.api.utils.auth_utils import get_password_hash

conn = psycopg2.connect(
    host='127.0.0.1',
    port=5432,
    database='mf_analytics',
    user='postgres',
    password='Babyaditya#007'
)
cur = conn.cursor()

# Run migration 025
try:
    with open('database/migrations/025_create_users_table.sql', 'r') as f:
        sql25 = f.read()
    cur.execute(sql25)
    conn.commit()
    print("Migration 025 executed.")
except Exception as e:
    conn.rollback()
    print("Migration 025 error (maybe exists):", e)

# Run migration 028
try:
    with open('database/migrations/028_secure_multi_user.sql', 'r') as f:
        sql28 = f.read()
    cur.execute(sql28)
    conn.commit()
    print("Migration 028 executed.")
except Exception as e:
    conn.rollback()
    print("Migration 028 error (maybe exists):", e)

# Now insert or update vinodkela
pwd_hash = get_password_hash('vinod123')
cur.execute("""
    INSERT INTO users (username, email, password_hash, role, permissions, is_active)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (username) DO UPDATE
    SET password_hash = EXCLUDED.password_hash,
        role = EXCLUDED.role,
        permissions = EXCLUDED.permissions,
        is_active = TRUE,
        failed_login_attempts = 0,
        locked_until = NULL
""", ('vinodkela', 'vinodkela@avfincorp.com', pwd_hash, 'admin', json.dumps(['all']), True))

conn.commit()
print("vinodkela user created/updated successfully!")

cur.execute("SELECT id, username, email, role, permissions, is_active FROM users WHERE username = 'vinodkela'")
print("User in DB:", cur.fetchone())

cur.close()
conn.close()
