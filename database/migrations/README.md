# Database Migrations

## Overview

This folder contains **numbered migration files** that define the database schema evolution over time.

**What is a migration?**
- A migration is a SQL script that makes changes to the database schema
- Each migration is numbered sequentially (001, 002, 003, ...)
- Migrations are run in order to build up the schema from scratch
- Migrations are **immutable** - once created, they should never be modified

**Why use migrations?**
- ✅ **Version Control**: Track schema changes over time
- ✅ **Reproducibility**: Anyone can recreate the database from scratch
- ✅ **Documentation**: Each migration explains what changed and why
- ✅ **Team Collaboration**: Multiple developers can work on schema safely
- ✅ **Rollback**: Can undo changes if needed (advanced)

---

## Migration Files

### 001_create_schema_v1.sql

**Description**: Initial schema creation for Mutual Fund Portfolio Analytics Platform

**Creates**:
- 6 tables: `amcs`, `schemes`, `periods`, `companies`, `scheme_snapshots`, `equity_holdings`
- All constraints (primary keys, foreign keys, check constraints, unique constraints)
- All indexes (for performance)
- All comments (for documentation)

**Status**: ✅ Production-Ready

**Run Once**: This migration should be run ONLY ONCE on a fresh database.

---

## How to Run Migrations

### Prerequisites

1. PostgreSQL 14+ is installed and running
2. Database `mf_analytics` exists
3. User `mf_admin` exists with privileges

---

### Run All Migrations (Fresh Database)

**From project root directory**:

```cmd
psql -U mf_admin -d mf_analytics -f database\migrations\001_create_schema_v1.sql
```

**Enter password when prompted**

**Expected output**:
```
BEGIN
CREATE TABLE
CREATE INDEX
COMMENT
... (many lines)
COMMIT
✅ Schema v1.0 applied successfully!
```

---

### Run Individual Migration

**If you need to run a specific migration**:

```cmd
psql -U mf_admin -d mf_analytics -f database\migrations\NNN_description.sql
```

Replace `NNN_description.sql` with the actual migration file name.

---

### Verify Migration Success

**Check tables exist**:

```sql
psql -U mf_admin -d mf_analytics
\dt
```

**Expected output**:
```
                List of relations
 Schema |       Name        | Type  |  Owner
--------+-------------------+-------+----------
 public | amcs              | table | mf_admin
 public | companies         | table | mf_admin
 public | equity_holdings   | table | mf_admin
 public | periods           | table | mf_admin
 public | scheme_snapshots  | table | mf_admin
 public | schemes           | table | mf_admin
(6 rows)
```

---

## How to Add New Migrations

### Step 1: Create Migration File

**Naming convention**: `NNN_description.sql`

**Example**: `002_add_fund_manager_column.sql`

**Template**:

```sql
-- ============================================================
-- MIGRATION NNN: DESCRIPTION
-- ============================================================
-- Description: What this migration does
-- Date: YYYY-MM-DD
-- Author: Your Name
-- Reason: Why this change is needed
-- ============================================================

BEGIN;

-- Your SQL changes here
ALTER TABLE schemes ADD COLUMN fund_manager VARCHAR(255);

COMMIT;

-- Success message
SELECT '✅ Migration NNN applied successfully!' AS status;
```

---

### Step 2: Document Migration

**Update this README**:

Add a new section under "Migration Files" with:
- Migration number and name
- Description of changes
- Date created
- Reason for change

---

### Step 3: Test Migration

**On development database**:

1. Run migration
2. Verify changes
3. Test application still works
4. Document any breaking changes

---

### Step 4: Commit to Git

**Add migration file to Git**:

```cmd
git add database/migrations/NNN_description.sql
git add database/migrations/README.md
git commit -m "Add migration NNN: description"
git push
```

---

## Migration Rules (IMPORTANT)

### ✅ DO

1. **Number sequentially**: 001, 002, 003, ...
2. **Use transactions**: Wrap changes in `BEGIN;` ... `COMMIT;`
3. **Add comments**: Explain what and why
4. **Test first**: Run on dev database before production
5. **Document**: Update this README
6. **Commit to Git**: Version control all migrations

### ❌ DO NOT

1. **Modify old migrations**: Once committed, migrations are immutable
2. **Skip numbers**: Always use next sequential number
3. **Run out of order**: Migrations must run in numerical order
4. **Delete migrations**: Even if mistake, create new migration to undo
5. **Run twice**: Each migration should be run only once

---

## Troubleshooting

### Error: "relation already exists"

**Cause**: Migration has already been run

**Solution**: 
- Check if tables already exist: `\dt`
- If tables exist, migration is already applied
- If you need to re-run, drop tables first (⚠️ DESTRUCTIVE)

---

### Error: "permission denied"

**Cause**: User doesn't have privileges

**Solution**:
```sql
-- As postgres superuser
GRANT ALL ON SCHEMA public TO mf_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO mf_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO mf_admin;
```

---

### Error: "syntax error"

**Cause**: SQL syntax is invalid

**Solution**:
- Check SQL syntax
- Test migration on dev database first
- Use `BEGIN;` and `ROLLBACK;` to test without committing

---

## Migration Tracking (Future)

**Currently**: Manual tracking (you know which migrations you've run)

**Future Enhancement**: Create a `schema_migrations` table to track which migrations have been applied:

```sql
CREATE TABLE schema_migrations (
    migration_id INTEGER PRIMARY KEY,
    migration_name VARCHAR(255) NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

This allows automated migration tools to know which migrations to run.

---

## Best Practices

1. **Keep migrations small**: One logical change per migration
2. **Test thoroughly**: Run on dev database first
3. **Document well**: Explain what and why
4. **Backup before migrating**: Always backup production database
5. **Plan for rollback**: Consider how to undo changes if needed
6. **Communicate**: Inform team before running migrations on shared databases

---

**Last Updated**: 2026-02-01  
**Current Schema Version**: 1.0  
**Total Migrations**: 1
