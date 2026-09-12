"""
PDF Attachment Manager.

Handles:
1. Streamed download of exchange PDFs with magic byte (%PDF) verification and 25MB ceiling.
2. HOT / WARM / PURGED 3-tier lifecycle management.
3. Daily purge of files older than 60 days to prevent disk accumulation.
4. Serving local files or issuing direct 302 redirect to exchange source.
"""

from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
import os
import hashlib
import requests
import psycopg2.extras

from src.db.connection import get_connection
from src.config import logger

PDF_STORAGE_BASE = os.path.join("data", "announcements", "pdfs")
MAX_PDF_SIZE_BYTES = 25 * 1024 * 1024  # 25 MB
DOWNLOAD_TIMEOUT = 12

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*",
}


class PDFManager:
    """Manages downloading, storing, and purging announcement attachments."""

    @staticmethod
    def _get_storage_path(year: int, month: int, filename: str) -> str:
        """Create relative storage path: data/announcements/pdfs/{YYYY}/{MM}/{filename}"""
        dir_path = os.path.join(PDF_STORAGE_BASE, str(year), f"{month:02d}")
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, filename)

    @staticmethod
    def download_attachment(attachment_id: int) -> bool:
        """
        Stream download a single attachment, verify %PDF magic bytes,
        compute SHA-256 hash, and update DB.
        """
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT attachment_id, exchange, original_file_name, 
                           source_file_url, created_at, download_attempts
                    FROM announcement_attachments
                    WHERE attachment_id = %s
                    """,
                    (attachment_id,)
                )
                att = cur.fetchone()

            if not att or not att.get("source_file_url"):
                return False

            url = att["source_file_url"]
            file_name = att.get("original_file_name") or f"announcement_{attachment_id}.pdf"
            # Ensure safe filename
            safe_name = f"{attachment_id}_{os.path.basename(file_name)}"
            dt = att["created_at"]
            target_path = PDFManager._get_storage_path(dt.year, dt.month, safe_name)

            headers = dict(DEFAULT_HEADERS)
            if att["exchange"] == "BSE":
                headers["Referer"] = "https://www.bseindia.com/"
            elif att["exchange"] == "NSE":
                headers["Referer"] = "https://www.nseindia.com/"

            # Stream download
            hasher = hashlib.sha256()
            total_bytes = 0
            is_valid_pdf = False

            with requests.get(url, headers=headers, stream=True, timeout=DOWNLOAD_TIMEOUT) as resp:
                if resp.status_code != 200:
                    PDFManager._record_download_failure(
                        attachment_id,
                        f"HTTP {resp.status_code}"
                    )
                    return False

                with open(target_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if not chunk:
                            continue

                        if total_bytes == 0:
                            # Verify magic bytes
                            if chunk.startswith(b"%PDF"):
                                is_valid_pdf = True
                            else:
                                PDFManager._record_download_failure(
                                    attachment_id,
                                    "Invalid magic bytes (not a PDF)"
                                )
                                f.close()
                                if os.path.exists(target_path):
                                    os.remove(target_path)
                                return False

                        total_bytes += len(chunk)
                        if total_bytes > MAX_PDF_SIZE_BYTES:
                            PDFManager._record_download_failure(
                                attachment_id,
                                f"File exceeded 25MB ceiling ({total_bytes} bytes)"
                            )
                            f.close()
                            if os.path.exists(target_path):
                                os.remove(target_path)
                            return False

                        hasher.update(chunk)
                        f.write(chunk)

            # Successfully saved
            sha256 = hasher.hexdigest()
            now_utc = datetime.now(timezone.utc)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE announcement_attachments
                    SET local_file_path = %s,
                        file_size_bytes = %s,
                        sha256_hash = %s,
                        is_locally_available = TRUE,
                        lifecycle_stage = 'HOT',
                        download_attempts = download_attempts + 1,
                        downloaded_at = %s,
                        download_error = NULL
                    WHERE attachment_id = %s
                    """,
                    (target_path, total_bytes, sha256, now_utc, attachment_id)
                )
            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            PDFManager._record_download_failure(attachment_id, str(e))
            logger.warning(f"Download failed for attachment {attachment_id}: {e}")
            return False

    @staticmethod
    def _record_download_failure(attachment_id: int, error_msg: str) -> None:
        """Update DB on download failure without interrupting collector."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE announcement_attachments
                    SET download_attempts = download_attempts + 1,
                        download_error = %s,
                        lifecycle_stage = CASE 
                            WHEN download_attempts >= 3 THEN 'FAILED' 
                            ELSE lifecycle_stage 
                        END
                    WHERE attachment_id = %s
                    """,
                    (error_msg[:255], attachment_id)
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to record download failure for {attachment_id}: {e}")

    @staticmethod
    def process_pending_downloads(batch_size: int = 10) -> int:
        """Fetch un-downloaded attachments and download them."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT attachment_id 
                    FROM announcement_attachments
                    WHERE is_locally_available = FALSE
                      AND lifecycle_stage = 'HOT'
                      AND download_attempts < 3
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (batch_size,)
                )
                ids = [r[0] for r in cur.fetchall()]

            success_count = 0
            for att_id in ids:
                if PDFManager.download_attachment(att_id):
                    success_count += 1
            return success_count
        except Exception as e:
            conn.rollback()
            logger.error(f"Error in process_pending_downloads: {e}")
            return 0

    @staticmethod
    def run_daily_purge() -> Dict[str, int]:
        """
        Transition attachments through lifecycle tiers:
        - Age > 7 days: HOT -> WARM
        - Age > 60 days: WARM -> PURGED (delete physical file, retain official URL)
        """
        conn = get_connection()
        warm_transitioned = 0
        purged_files = 0
        now_utc = datetime.now(timezone.utc)

        try:
            with conn.cursor() as cur:
                # 1. HOT -> WARM (> 7 days)
                cur.execute(
                    """
                    UPDATE announcement_attachments
                    SET lifecycle_stage = 'WARM'
                    WHERE lifecycle_stage = 'HOT'
                      AND created_at < NOW() - INTERVAL '7 days'
                    """
                )
                warm_transitioned = cur.rowcount

                # 2. WARM/HOT -> PURGED (> 60 days)
                cur.execute(
                    """
                    SELECT attachment_id, local_file_path
                    FROM announcement_attachments
                    WHERE is_locally_available = TRUE
                      AND created_at < NOW() - INTERVAL '60 days'
                    """
                )
                to_purge = cur.fetchall()

                for att_id, path in to_purge:
                    if path and os.path.exists(path):
                        try:
                            os.remove(path)
                        except OSError as e:
                            logger.warning(f"Failed to delete purged file {path}: {e}")

                    cur.execute(
                        """
                        UPDATE announcement_attachments
                        SET is_locally_available = FALSE,
                            lifecycle_stage = 'PURGED',
                            purged_at = %s,
                            local_file_path = NULL
                        WHERE attachment_id = %s
                        """,
                        (now_utc, att_id)
                    )
                    purged_files += 1

            conn.commit()
            logger.info(f"PDF Lifecycle purge complete: {warm_transitioned} warmed, {purged_files} purged")
            return {"warmed": warm_transitioned, "purged": purged_files}

        except Exception as e:
            conn.rollback()
            logger.error(f"Error in run_daily_purge: {e}")
            return {"warmed": 0, "purged": 0}

    @staticmethod
    def get_attachment_location(attachment_id: int) -> Optional[Dict[str, Any]]:
        """
        Determine how an attachment should be delivered:
        - If local file exists: return local path
        - Otherwise: return exchange URL for 302 redirect
        """
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT attachment_id, exchange, original_file_name,
                           source_file_url, local_file_path, is_locally_available,
                           lifecycle_stage, mime_type
                    FROM announcement_attachments
                    WHERE attachment_id = %s
                    """,
                    (attachment_id,)
                )
                row = cur.fetchone()

            if not row:
                return None

            # If locally available and file actually exists on disk
            if row["is_locally_available"] and row.get("local_file_path"):
                if os.path.exists(row["local_file_path"]):
                    return {
                        "delivery": "LOCAL",
                        "path": os.path.abspath(row["local_file_path"]),
                        "filename": row["original_file_name"],
                        "mime_type": row["mime_type"] or "application/pdf"
                    }

            # Fallback to direct exchange URL
            return {
                "delivery": "REDIRECT",
                "url": row["source_file_url"],
                "filename": row["original_file_name"]
            }
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to resolve attachment location for {attachment_id}: {e}")
            return None
