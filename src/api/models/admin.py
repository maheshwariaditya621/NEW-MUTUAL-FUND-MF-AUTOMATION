from pydantic import BaseModel, Field
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
