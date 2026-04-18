from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional, Dict, Any
from datetime import datetime

class PendingMerge(BaseModel):
    merge_id: int
    amc_id: int
    amc_name: str
    new_scheme_name: str
    old_scheme_id: int
    old_scheme_name: str
    plan_type: str
    option_type: str
    is_reinvest: bool
    confidence_score: float
    detection_method: str
    metadata: Dict[str, Any]
    created_at: datetime
    status: str

class NotificationLog(BaseModel):
    notification_id: int
    level: str
    category: str
    content: str
    status: str
    created_at: datetime

class AdminStats(BaseModel):
    pending_merges_count: int
    total_schemes: int
    last_extraction_status: str
    error_count_24h: int

# --- User Management Models ---

class UserManagementRead(BaseModel):
    id: int
    username: str
    email: str
    role: str
    is_active: bool
    permissions: List[str]
    expires_at: Optional[datetime]
    last_login: Optional[datetime]
    created_at: datetime
    failed_login_attempts: int
    locked_until: Optional[datetime]

class UserCreate(BaseModel):
    username: str
    email: EmailStr
    password: str
    role: str = "user"
    permissions: List[str] = []
    expires_at: Optional[datetime] = None

class UserUpdate(BaseModel):
    username: Optional[str] = None
    email: Optional[EmailStr] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None
    permissions: Optional[List[str]] = None
    expires_at: Optional[datetime] = None
    password: Optional[str] = None # For password reset

class InviteCreate(BaseModel):
    email: EmailStr
    permissions: List[str] = []
    days_valid: int = 7
    account_expiry_days: Optional[int] = 30

class InviteRead(BaseModel):
    invite_id: int
    token: str
    email: str
    permissions: List[str]
    expires_at: datetime
    account_expiry_days: Optional[int]
    is_used: bool
    created_at: datetime
