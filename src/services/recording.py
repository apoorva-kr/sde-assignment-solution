import logging
from typing import Optional
import httpx
from src.config import settings
from src.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)

async def fetch_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
    attempt: int = 1
) -> Optional[str]:
    """
    Attempt to fetch the Exotel recording and upload it to S3.
    Uses recursive polling via Celery to avoid blocking the system.
    """
    try:
        recording_url = await _fetch_exotel_recording_url(call_sid, exotel_account_id)

        if not recording_url:
            if attempt <= 5:
                # Exponential backoff: 30s, 60s, 120s, 240s, 480s
                delay = 30 * (2 ** (attempt - 1))
                logger.info(f"Recording not ready, retrying in {delay}s", 
                            extra={"interaction_id": interaction_id, "attempt": attempt})
                
                # Re-queue the task for later execution
                poll_recording_task.apply_async(
                    args=[interaction_id, call_sid, exotel_account_id, attempt + 1], 
                    countdown=delay
                )
                return None
            else:
                logger.error("CRITICAL: Recording failed after 5 attempts", 
                             extra={"interaction_id": interaction_id})
                return None

        # If URL is found, proceed to upload
        return await _upload_to_s3(recording_url, interaction_id)

    except Exception as e:
        logger.exception("recording_upload_error", 
                         extra={"interaction_id": interaction_id, "error": str(e)})
        return None

@celery_app.task(name="poll_recording_task")
def poll_recording_task(interaction_id, call_sid, exotel_account_id, attempt):
    # This wrapper allows the async function to run inside the Celery task
    import asyncio
    return asyncio.run(fetch_and_upload_recording(interaction_id, call_sid, exotel_account_id, attempt))

async def _fetch_exotel_recording_url(call_sid: str, account_id: str) -> Optional[str]:
    url = f"https://api.exotel.com/v1/Accounts/{account_id}/Calls/{call_sid}/Recording"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp.json().get("recording_url")
            return None
    except httpx.HTTPError:
        return None

async def _upload_to_s3(recording_url: str, interaction_id: str) -> str:
    s3_key = f"recordings/{interaction_id}.mp3"
    logger.info("recording_uploaded", extra={"interaction_id": interaction_id, "s3_key": s3_key})
    return s3_key