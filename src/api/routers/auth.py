from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from psycopg2.extensions import cursor
from datetime import timedelta

from src.api.models.auth import Token, UserResponse
from src.api.utils.auth_utils import verify_password, create_access_token
from src.api.dependencies import get_db_cursor, get_current_user
from src.config import ACCESS_TOKEN_EXPIRE_MINUTES, logger

router = APIRouter()

@router.post("/login", response_model=Token)
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    cur: cursor = Depends(get_db_cursor)
):
    """
    Authenticate a user and return a JWT access token.
    Uses OAuth2 password flow with form data.
    """
    # Fetch user from database
    cur.execute(
        "SELECT id, username, password_hash, is_active FROM users WHERE username = %s",
        (form_data.username,)
    )
    user = cur.fetchone()
    
    if not user or not verify_password(form_data.password, user[2]):
        logger.warning(f"Failed login attempt for user: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if not user[3]: # is_active
        raise HTTPException(status_code=400, detail="Inactive user")
    
    # Update last login time
    cur.execute(
        "UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = %s",
        (user[0],)
    )
    
    # Create access token
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user[1]}, expires_delta=access_token_expires
    )
    
    logger.info(f"Successful login for user: {user[1]}")
    return {
        "access_token": access_token, 
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60
    }

@router.get("/me", response_model=UserResponse)
async def read_users_me(current_user: dict = Depends(get_current_user)):
    """
    Get information about the currently authenticated user.
    """
    return current_user
