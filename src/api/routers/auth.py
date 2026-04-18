from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.security import OAuth2PasswordRequestForm
from psycopg2.extensions import cursor
from datetime import datetime, timedelta, timezone
from src.api.models.auth import Token, UserResponse
from src.api.utils.auth_utils import verify_password, create_access_token, verify_totp
from src.api.dependencies import get_db_cursor, get_current_user
from src.config import ACCESS_TOKEN_EXPIRE_MINUTES, logger
from typing import Optional

router = APIRouter()

@router.post("/login", response_model=Token)
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    totp_code: Optional[str] = Query(None),
    cur: cursor = Depends(get_db_cursor)
):
    """
    Authenticate a user. Handles:
    - Standard OAuth2 password flow
    - Account expiry & manual deactivation
    - Brute-force protection (Auto-lock after 5 fails)
    - Two-Factor Authentication (TOTP)
    """
    # Fetch user from database with all security fields
    cur.execute(
        """SELECT id, username, password_hash, is_active, expires_at, 
                  failed_login_attempts, locked_until, is_totp_enabled, totp_secret
           FROM users WHERE username = %s""",
        (form_data.username,)
    )
    user = cur.fetchone()
    
    if not user:
        logger.warning(f"Login attempted for non-existent user: {form_data.username}")
        raise HTTPException(status_code=401, detail="Incorrect username or password")

    user_id, username, password_hash, is_active, expires_at, fails, locked_until, totp_enabled, totp_secret = user

    # 1. Lock Check (Brute-force)
    if locked_until and datetime.now(timezone.utc) < locked_until:
        raise HTTPException(
            status_code=403, 
            detail="Account is locked due to frequent failed attempts. Please try again later."
        )

    # 2. Status & Expiry Checks
    if not is_active:
        raise HTTPException(status_code=403, detail="Account deactivated. Contact administrator.")
    
    if expires_at and datetime.now(timezone.utc) > expires_at:
        raise HTTPException(
            status_code=403, 
            detail="Your access period has ended. Please contact the administrator to renew your access."
        )

    # 3. Password Verification
    if not verify_password(form_data.password, password_hash):
        # Increment failed attempts
        new_fails = fails + 1
        new_lock = None
        if new_fails >= 5:
            new_lock = datetime.now(timezone.utc) + timedelta(minutes=30)
            logger.critical(f"AUTO-LOCK: User {username} locked for 30m due to 5 failures.")
            
        cur.execute(
            "UPDATE users SET failed_login_attempts = %s, locked_until = %s WHERE id = %s",
            (new_fails, new_lock, user_id)
        )
        
        detail = "Incorrect username or password"
        if new_fails >= 3:
            detail += f" ({5 - new_fails} attempts remaining)"
            
        raise HTTPException(status_code=401, detail=detail)

    # 4. TOTP (2FA) Check
    if totp_enabled:
        if not totp_code:
            # Tell frontend 2FA is required
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="2FA_REQUIRED",
                headers={"X-2FA-Status": "Required"}
            )
        
        if not verify_totp(totp_secret, totp_code):
            raise HTTPException(status_code=401, detail="Invalid 2FA code")

    # 5. Success - Reset security counters & update last login
    cur.execute(
        "UPDATE users SET last_login = CURRENT_TIMESTAMP, failed_login_attempts = 0, locked_until = NULL WHERE id = %s",
        (user_id,)
    )
    
    # Create access token
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": username}, expires_delta=access_token_expires
    )
    
    logger.info(f"Successful login for user: {username}")
    return {
        "access_token": access_token, 
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60
    }

from pydantic import BaseModel

class PasswordChangeRequest(BaseModel):
    current_password: str
    new_password: str

@router.post("/change-password")
async def change_password(
    req: PasswordChangeRequest,
    current_user: dict = Depends(get_current_user),
    cur: cursor = Depends(get_db_cursor)
):
    """Allow a logged-in user to change their own password."""
    # 1. Fetch current password hash
    cur.execute("SELECT password_hash FROM users WHERE id = %s", (current_user["id"],))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
        
    # 2. Verify current password
    if not verify_password(req.current_password, row[0]):
        raise HTTPException(status_code=400, detail="Incorrect current password")
        
    # 3. Hash and update new password
    from src.api.utils.auth_utils import get_password_hash
    hashed = get_password_hash(req.new_password)
    cur.execute("UPDATE users SET password_hash = %s WHERE id = %s", (hashed, current_user["id"]))
    
    return {"status": "success", "message": "Password updated successfully"}

@router.get("/me", response_model=UserResponse)
async def read_users_me(current_user: dict = Depends(get_current_user)):
    """
    Get information about the currently authenticated user.
    """
    return current_user
