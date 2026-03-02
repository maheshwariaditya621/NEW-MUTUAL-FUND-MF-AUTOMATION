# AWS SIMPLE DEPLOYMENT PLAN
### Mutual Fund Portfolio Analytics Platform
> Budget target: ₹0–₹1,000/month. Written for non-technical founders. Updated: March 2026.

---

## Table of Contents
1. [Deployment Philosophy](#1-deployment-philosophy)
2. [Recommended Setup](#2-recommended-setup)
3. [Domain Setup](#3-domain-setup)
4. [Step-by-Step Deployment Guide](#4-step-by-step-deployment-guide)
5. [Database Hosting Decision](#5-database-hosting-decision)
6. [Background Services Setup](#6-background-services-setup)
7. [Backup Plan](#7-backup-plan)
8. [Testing Deployment](#8-testing-deployment)
9. [Failure & Risk Analysis](#9-failure--risk-analysis)
10. [Scaling Path (Future)](#10-scaling-path-future)

---

## 1. Deployment Philosophy

### Why EC2?

An EC2 instance is simply a **virtual computer that lives in Amazon's data center**. You rent it by the hour, it runs 24/7, and it gives you full control — just like your own server.

For a 5–10 person team with a publicly accessible site, EC2 is the best choice because:
- **Cheapest option**: as low as ₹0/month on Free Tier or ~₹600–900/month for a small paid instance
- **Everything on one machine**: backend, frontend, database, and scheduler all run on the same server — no complexity
- **You own it**: no vendor lock-in, no magic abstractions. If something breaks, you can SSH in and fix it
- **Full control**: you decide what runs, when, and how

### Why NOT Complex Architecture?

| Complex Option | Why We're Skipping It |
|---------------|----------------------|
| Kubernetes | Overkill for 5 users; requires 2+ engineers to manage |
| ECS/Fargate | Extra cost and complexity; solves problems we don't have |
| Load Balancer | Needed only when you have 100+ concurrent users |
| RDS | Fine for later; EC2-hosted Postgres works well at our scale |
| CI/CD Pipeline | Useful later; for now, `git pull` + restart is sufficient |
| Lambda Functions | Not suited for long-running scheduler and downloader jobs |

### The Simple Truth

The site is publicly reachable on the internet, but protected by login so only your team can use it. For that you need:
- One server that runs all day ✅
- A domain name + HTTPS ✅
- A username/password login (HTTP Basic Auth or JWT) ✅
- Data backed up regularly ✅
- Services that restart if they crash ✅

That's it. We don't need anything else right now.

---

## 2. Recommended Setup

### Final Recommendation

| Component | Choice | Reason |
|-----------|--------|--------|
| **Server** | AWS EC2 `t3.micro` | Free Tier eligible; enough for 5–10 users |
| **Operating System** | Ubuntu 22.04 LTS | Most stable, widely supported, free |
| **Storage (Disk)** | 20 GB EBS (SSD) | Enough for data, code, logs, backups |
| **Region** | `ap-south-1` (Mumbai) | Lowest latency for Indian users |
| **IP** | Elastic IP (static) | Free when attached to running instance |

### Cost Estimate (Monthly)

| Item | Free Tier | After Free Tier |
|------|-----------|----------------|
| EC2 t3.micro (750 hrs/month) | ₹0 for 12 months | ~₹600–700/month |
| EBS Storage 20 GB | ₹0 for 12 months | ~₹80–100/month |
| Elastic IP (static IP) | ₹0 if attached | ₹0 if instance is running |
| Data Transfer (minimal internal use) | ~₹0 | ~₹0–50/month |
| **TOTAL** | **₹0/month (first 12 months)** | **~₹700–850/month** |

> ✅ **Well within your ₹1,000/month budget.**

### AWS Free Tier Note
- If your AWS account is less than 12 months old: **t3.micro is completely free** for 750 hours/month
- After 12 months: cost is ~₹700–850/month
- Always set **billing alerts** in AWS so you never get surprise charges

---

## 3. Domain Setup

### Connecting Your Domain to EC2

You already own a domain. Here's how to point it to your server:

**Step 1: Get your EC2's public IP (Elastic IP)**
- In AWS console → EC2 → Elastic IPs → Allocate → Associate with your instance
- You'll get a fixed IP like `13.235.100.50`

**Step 2: Add A Record in your domain registrar's DNS panel**

| Field | Value |
|-------|-------|
| Type | A |
| Name | `app` (for app.yourdomain.com) or `@` (for yourdomain.com) |
| Value | Your EC2 Elastic IP (e.g. `13.235.100.50`) |
| TTL | 300 (5 minutes) |

> 📍 Where to do this: Login to your domain registrar (GoDaddy / Namecheap / BigRock / Registrar). Find "DNS Management" or "Manage DNS".

**Step 3: Wait 5–30 minutes** for DNS to propagate worldwide.

**Step 4: Verify**
```bash
nslookup app.yourdomain.com
# Should return your EC2 IP address
```

### Should You Use Route 53?
- **Route 53** is Amazon's DNS service → costs ~₹50/month per hosted zone
- **Recommendation**: Keep using your existing domain registrar's DNS (it's free and works the same way)
- Only switch to Route 53 if your registrar is unreliable or you need advanced routing

### SSL / HTTPS (Free with Let's Encrypt)

HTTPS is required so that:
- Browser doesn't show "Not Secure" warning
- Data is encrypted in transit

We use **Let's Encrypt** — it's completely free, automated, and renews every 90 days.

Setup command (run on server after Nginx is installed):
```bash
sudo apt install certbot python3-certbot-nginx
sudo certbot --nginx -d app.yourdomain.com
```

---

## 4. Step-by-Step Deployment Guide

> 💡 **Before starting**: Launch an EC2 t3.micro instance with Ubuntu 22.04 from the AWS console. Download the `.pem` key file when prompted. Attach an Elastic IP.

---

### Step 1: SSH into Your Server

```bash
# From your laptop (Windows: use PowerShell or MobaXterm)
ssh -i your-key.pem ubuntu@13.235.100.50
```

*What this does: Connects you to your remote server's terminal. Replace `13.235.100.50` with your actual Elastic IP.*

---

### Step 2: Update the Server and Install Base Tools

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git wget curl unzip nginx python3 python3-pip python3-venv
sudo apt install -y libreoffice-calc
```

*What this does: Updates the OS, installs Git (for pulling your code), Nginx (web server), and Python.*

---

### Step 3: Install PostgreSQL (Database)

```bash
sudo apt install -y postgresql postgresql-contrib

# Start and enable PostgreSQL
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

*What this does: Installs the database engine and makes sure it starts automatically on server boot.*

---

### Step 4: Create the Database

```bash
# Switch to postgres user
sudo -u postgres psql

# Inside postgres console:
CREATE DATABASE mf_analytics;
CREATE USER mf_admin WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE mf_analytics TO mf_admin;
\q
```

---

### Step 5: Clone Your Git Repository

```bash
# On the server, go to home directory
cd /home/ubuntu

# Clone your project
git clone https://github.com/your-org/your-repo.git mf-app
cd mf-app
```

*What this does: Downloads your project code onto the server.*

---

### Step 6: Create Python Virtual Environment & Install Dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium
playwright install-deps
```

*What this does: Creates an isolated Python environment and installs all required libraries. (Note: High-fidelity Excel handling is powered by the `libreoffice-calc` system package you installed in Step 2).*

---

### Step 7: Set Up Environment Variables

```bash
# Create the .env file from template
cp .env.example .env

# Edit it with your actual values
nano .env
```

Fill in:
- DB_HOST=localhost
- DB_NAME=mf_analytics
- DB_USER=mf_admin
- DB_PASSWORD=your_actual_password
- TELEGRAM_BOT_TOKEN=...
- (Save: Ctrl+O, Enter, Ctrl+X)

---

### Step 8: Load Database Schema

```bash
source .venv/bin/activate
psql -U mf_admin -d mf_analytics -h localhost -f database/schema_v1.0.sql
```

*What this does: Creates all the tables in your database based on the schema file.*

---

### Step 9: Build the Frontend

```bash
# Install Node.js
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash
source ~/.bashrc
nvm install 18
nvm use 18

# Build frontend
cd frontend
npm install
npm run build
cd ..
```

*What this does: Compiles the React app into static HTML/CSS/JS files that Nginx can serve. The built files go to `frontend/dist/`.*

---

### Step 10: Configure Nginx (Reverse Proxy + Frontend)

```bash
sudo nano /etc/nginx/sites-available/mf-app
```

Paste this configuration:

```nginx
server {
    listen 80;
    server_name app.yourdomain.com;

    # Serve React frontend static files
    location / {
        # Basic login for all 5-10 team members (public site, restricted access)
        auth_basic "Team Access";
        auth_basic_user_file /etc/nginx/.htpasswd;

        root /home/ubuntu/mf-app/frontend/dist;
        try_files $uri $uri/ /index.html;
    }

    # Forward /api/ requests to FastAPI backend (also protected by auth above)
    location /api/ {
        auth_basic "Team Access";
        auth_basic_user_file /etc/nginx/.htpasswd;

        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

> 🔐 **Important**: Since this is a public website, the login is mandatory. Create one account per team member:
> ```bash
> sudo apt install apache2-utils
> sudo htpasswd -c /etc/nginx/.htpasswd aditya     # creates file + first user
> sudo htpasswd /etc/nginx/.htpasswd teammate2      # add more users
> sudo htpasswd /etc/nginx/.htpasswd teammate3
> sudo nginx -t && sudo systemctl reload nginx
> ```
> Each person gets their own browser password prompt when visiting the site.

```bash
# Enable the site
sudo ln -s /etc/nginx/sites-available/mf-app /etc/nginx/sites-enabled/
sudo nginx -t       # Test config (should say "ok")
sudo systemctl restart nginx
```

*What this does: Tells Nginx to serve your website on port 80. `/api/` requests go to FastAPI; everything else serves the React app. All pages require a login.*

---

### Step 11: Set Up SSL (HTTPS)

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d app.yourdomain.com
```

Follow the prompts. SSL is now active and auto-renews every 90 days.

---

### Step 12: Set Up Backend as System Service

See Section 6 for the full systemd service setup.

---

## 5. Database Hosting Decision

### Option A: PostgreSQL on the Same EC2 (Recommended for Now)

```
EC2 Server
├── FastAPI Backend
├── Scheduler
├── PostgreSQL ← same machine
└── Nginx (Frontend)
```

| Factor | Details |
|--------|---------|
| **Cost** | ₹0 extra (already paying for EC2) |
| **Complexity** | Very simple — all on one machine |
| **Risk** | If EC2 fails, DB goes too (mitigated by daily backups) |
| **Performance** | Excellent for < 10 users |
| **Setup** | 5 minutes |

**Verdict: Use this. It's the right choice at your scale.**

---

### Option B: AWS RDS (Managed Database)

```
EC2 Server          RDS Instance (separate)
├── FastAPI    →    PostgreSQL (managed)
├── Scheduler
└── Nginx
```

| Factor | Details |
|--------|---------|
| **Cost** | +₹700–1,500/month extra |
| **Complexity** | Much higher — separate service to configure |
| **Risk** | Lower — DB survives if EC2 dies |
| **Performance** | Better for 50+ concurrent users |
| **Benefit** | Auto-backups, multi-AZ failover, automated patching |

**Verdict: Not needed yet. Upgrade to RDS when you have 50+ users or your team demands 99.9% uptime.**

---

## 6. Background Services Setup

Services that need to run 24/7 must be configured as **systemd services**. This means they:
- Start automatically when the server boots
- Restart automatically if they crash
- Have logs you can view at any time

### FastAPI Backend Service

```bash
sudo nano /etc/systemd/system/mf-api.service
```

```ini
[Unit]
Description=Mutual Fund Analytics API
After=network.target postgresql.service

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/mf-app
Environment=PATH=/home/ubuntu/mf-app/.venv/bin
ExecStart=/home/ubuntu/mf-app/.venv/bin/python run_api.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable mf-api
sudo systemctl start mf-api
sudo systemctl status mf-api   # Should show "active (running)"
```

---

### Scheduler Service

```bash
sudo nano /etc/systemd/system/mf-scheduler.service
```

```ini
[Unit]
Description=Mutual Fund Scheduler
After=network.target postgresql.service

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/mf-app
Environment=PATH=/home/ubuntu/mf-app/.venv/bin
ExecStart=/home/ubuntu/mf-app/.venv/bin/python -m src.scheduler.main
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable mf-scheduler
sudo systemctl start mf-scheduler
```

---

### Viewing Service Logs

```bash
# Backend API logs (live)
sudo journalctl -u mf-api -f

# Scheduler logs (live)
sudo journalctl -u mf-scheduler -f

# Last 100 lines
sudo journalctl -u mf-api -n 100
```

---

### Restarting Services

```bash
sudo systemctl restart mf-api
sudo systemctl restart mf-scheduler
sudo systemctl restart nginx
sudo systemctl restart postgresql
```

---

## 7. Backup Plan

### Daily Database Backup Script

Create the script:
```bash
nano /home/ubuntu/backup_db.sh
```

```bash
#!/bin/bash
# Daily PostgreSQL Backup Script

BACKUP_DIR="/home/ubuntu/mf-app/backups"
DATE=$(date +%Y%m%d_%H%M%S)
FILENAME="mf_analytics_${DATE}.sql"

mkdir -p $BACKUP_DIR

# Create backup
pg_dump -U mf_admin -h localhost mf_analytics > "$BACKUP_DIR/$FILENAME"

# Keep only last 7 days of backups (auto-cleanup)
find $BACKUP_DIR -name "*.sql" -mtime +7 -delete

echo "Backup complete: $FILENAME"
```

```bash
chmod +x /home/ubuntu/backup_db.sh
```

### Schedule Backup Daily via Cron

```bash
crontab -e

# Add this line (runs at 2 AM every night):
0 2 * * * /home/ubuntu/backup_db.sh >> /home/ubuntu/mf-app/logs/backup.log 2>&1
```

---

### Where Backups Are Stored

- **Primary**: `/home/ubuntu/mf-app/backups/` on the EC2 server
- **Secondary (recommended)**: Copy backups to S3 once a week

Add this to the backup script to also push to S3 (optional, costs <₹10/month):
```bash
aws s3 cp "$BACKUP_DIR/$FILENAME" s3://your-bucket-name/db-backups/
```

---

### How to Restore a Backup

```bash
# Stop the API first
sudo systemctl stop mf-api

# Restore database
psql -U mf_admin -d mf_analytics -h localhost < /home/ubuntu/mf-app/backups/mf_analytics_20251101_020000.sql

# Restart API
sudo systemctl start mf-api
```

---

### What Happens If the Server Dies Entirely?

1. Launch a new EC2 t3.micro instance
2. Re-run Steps 2–11 from the deployment guide
3. Download your latest backup from S3 (or your local copy)
4. Restore it with the command above
5. Re-upload your `.env` file

**Estimated recovery time**: 2–4 hours  
**Data loss**: Maximum 24 hours (last backup)

---

### Disk Space Management (Admin File Manager)

The server's 20 GB disk fills up over time as raw downloads and merged Excels accumulate. Once data is confirmed loaded into the database, these files are no longer needed.

**How to free space without SSH**:
1. Login to your website → **Admin Panel → File Management**
2. Check the **DB Loaded** column — only delete rows where it shows 📥 (Loaded)
3. Click **Delete Raw** or **Delete Merged** to free space per AMC+month
4. Use **Bulk Wipe** to wipe an entire past month across all AMCs in one click

**Monthly disk cleanup routine** (recommended after each monthly extraction):
> After confirming all AMCs for the month are loaded → open Admin Panel File Management → bulk wipe raw files for that month → saves 1–2 GB per month cycle.

**If disk is already full (emergency)**:
```bash
# SSH in and check usage
df -h

# Find the biggest folders
du -sh /home/ubuntu/mf-app/data/*

# Delete a specific raw folder manually if Admin Panel is unreachable
rm -rf /home/ubuntu/mf-app/data/raw/hdfc/2025/11
```

---

## 8. Testing Deployment

Run through this checklist every time you deploy or update the server:

### Checklist

| Test | What to Do | Expected Result |
|------|-----------|-----------------|
| 🌐 **Server reachable** | `ping 13.235.100.50` | Gets a response |
| 🔒 **HTTPS working** | Open `https://app.yourdomain.com` in browser | No "Not Secure" warning; padlock icon |
| 🔐 **Login prompt working** | Open site in Incognito window | Browser shows username/password popup |
| 🌍 **Domain resolving** | `nslookup app.yourdomain.com` | Returns your EC2 IP |
| ⚡ **API responding** | `curl http://localhost:8000/docs` on server | Returns HTML page |
| 📊 **API data returning** | Open `https://app.yourdomain.com/api/amcs` | Returns JSON list of AMCs |
| 💾 **DB writable** | `psql -U mf_admin -d mf_analytics -c "SELECT 1;"` | Returns "1 row" |
| 🔄 **Scheduler active** | `sudo systemctl status mf-scheduler` | Shows "active (running)" |
| 🎨 **Frontend loading** | Open `https://app.yourdomain.com` after login | Website renders without blank page |
| 🗃️ **File Manager working** | Open Admin Panel → File Management | Inventory table loads with file statuses |
| 🔔 **Telegram alerts** | Check your Telegram channel | Messages arriving from bot |

### How to Test Each

**Server reachable (from your laptop)**:
```bash
ping 13.235.100.50
```

**HTTPS working**:
Just open your URL in Chrome. If no warning and padlock is green, it's working.

**API responding (SSH into server, then)**:
```bash
curl http://localhost:8000/docs
```

**DB writable (SSH into server, then)**:
```bash
psql -U mf_admin -d mf_analytics -h localhost -c "SELECT COUNT(*) FROM amcs;"
```

**Scheduler active**:
```bash
sudo systemctl status mf-scheduler
```

---

## 9. Failure & Risk Analysis

| Risk | What Happens | Severity | Recovery Plan |
|------|-------------|----------|---------------|
| **EC2 instance stopped** | Website goes offline; all services stop | 🔴 Critical | SSH in and start instance; all services auto-start via systemd |
| **AWS billing issue** | AWS stops your EC2 after payment failure | 🔴 Critical | Pay immediately; instance data preserved for 30 days |
| **Data loss (disk failure)** | All data on EC2 lost | 🔴 Critical | Restore from S3 backups; re-deploy; estimated 2–4 hr recovery |
| **Database corruption** | API returns errors; website breaks | 🔴 Critical | Stop API; restore from yesterday's backup |
| **Misconfigured DNS** | Domain doesn't reach server | 🟠 High | Fix A record in domain registrar; wait 30 min for propagation |
| **SSL certificate expired** | Browser shows "Not Secure" | 🟠 High | Run `sudo certbot renew`; auto-renewal should prevent this |
| **Memory overload** | EC2 runs out of RAM; processes crash | 🟠 High | SSH in; restart services; investigate memory leak; upgrade to t3.small |
| **Disk full** | Downloads fail; DB writes fail; site errors | 🟠 High | Admin Panel → File Management → delete old raw/merged files; or SSH: `rm -rf /home/ubuntu/mf-app/data/raw/<amc>/<year>/<month>` |
| **Admin deletes files before DB load** | Source data lost; must re-download from AMC | 🟠 High | Re-run downloader for those AMC+months; check DB Loaded column before deleting |
| **AMC downloader fails** | That month's data not collected | 🟡 Medium | Fix downloader script; re-run manually |
| **Scheduler stops** | Monthly jobs don't trigger automatically | 🟡 Medium | `sudo systemctl restart mf-scheduler`; run missed jobs manually |
| **Nginx crash** | Website unreachable (even though API is running) | 🟡 Medium | `sudo systemctl restart nginx` |
| **Code deployment error** | Website shows errors after update | 🟡 Medium | `git revert` to previous commit; restart services |
| **Telegram bot stopped** | No alerts sent | 🟢 Low | Restart; check token validity in `.env` |

### Prevention Tips

- ✅ Set AWS billing alert at ₹800/month (before hitting limit)
- ✅ Set up Telegram alerts for service crashes
- ✅ Enable systemd auto-restart on all services
- ✅ Run daily backups (automated via cron)
- ✅ Check disk usage weekly (alert if > 70%)
- ✅ Keep a copy of `.env` and backups on your laptop or S3

---

## 10. Scaling Path (Future)

This section tells you **when and what to upgrade** — don't do any of this prematurely.

```
Current: 5–10 users, all on one EC2 machine
          │
          ▼ When you hit 50+ users or need 99.9% uptime
         Add RDS (database on separate server)
          │
          ▼ When you hit 100+ users or page load is slow
         Add Nginx caching + frontend CDN (CloudFront)
          │  
          ▼ When you hit 500+ users or multiple teams
         Split backend to separate EC2
         Add Application Load Balancer
          │
          ▼ Only if you go fully public (1000+ users)
         Consider ECS / Fargate / auto-scaling groups
```

### Detailed Upgrade Steps

| When | What to Upgrade | How | Estimated Cost Increase |
|------|----------------|-----|------------------------|
| 50+ concurrent users | Upgrade to `t3.small` | Change instance type in EC2 console | +₹600/month |
| Need better uptime | Move DB to RDS | Export DB → import to RDS; change DB_HOST in .env | +₹700–1500/month |
| Frontend is slow | Add CloudFront CDN | Point CloudFront to your domain | +₹50–200/month |
| Backend is slow | Upgrade to `t3.medium` | Change instance type | +₹1,200/month |
| Need zero downtime | Add Load Balancer + 2 EC2s | Major architecture change | +₹2,000+/month |

### What to Upgrade First (Priority Order)

1. **First**: Add RDS when you want your database to survive a server crash
2. **Second**: Upgrade EC2 size (t3.small → t3.medium) when pages are slow
3. **Third**: Add Nginx caching to speed up repeated API calls
4. **Fourth**: Use CloudFront for frontend static files
5. **Last**: Load balancer + multiple EC2s (only for 500+ users)

> 💡 **Founder principle**: Don't scale infrastructure ahead of the problem. Every step above costs money and adds complexity. Upgrade only when you actually feel the pain of the current setup.

---

*Document maintained by the engineering team. Last updated: March 2026.*
