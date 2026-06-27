import asyncio
import logging
from datetime import datetime
from typing import Any, Dict

from src.tasks.celery_app import celery_app
from src.services.post_call_processor import PostCallProcessor, PostCallContext
from src.services.recording import fetch_and_upload_recording
from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage
from src.services.rate_limiter import RateLimiter
from src.services.metrics import metrics_tracker

logger = logging.getLogger(__name__)

@celery_app.task(
    name="process_interaction_end_background_task",
    bind=True,
    max_retries=5,
    queue="postcall_processing",
)
def process_interaction_end_background_task(self, payload: Dict[str, Any]):
    """
    Main Celery task. Decoupled and rate-limit aware.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # 1. Trigger recording poller (Fire-and-forget, non-blocking)
        # We don't 'await' this; let it manage itself via its own retry logic
        loop.run_until_complete(fetch_and_upload_recording(
            payload["interaction_id"], 
            payload.get("call_sid", ""), 
            payload.get("exotel_account_id", "")
        ))

        # 2. Run LLM Analysis
        loop.run_until_complete(_process_llm_analysis(payload))

    except Exception as e:
        logger.exception("celery_task_failed", extra={"interaction_id": payload.get("interaction_id")})
        raise self.retry(exc=e, countdown=60)
    finally:
        loop.close()

async def _process_llm_analysis(payload: Dict[str, Any]):
    interaction_id = payload["interaction_id"]
    limiter = RateLimiter()

    # Rate Limit Check before LLM call
    if not limiter.is_allowed(payload["customer_id"]):
        logger.warning(f"Rate limit hit for {payload['customer_id']}. Retrying.")
        raise Exception("Rate limit exceeded")

    ctx = PostCallContext(
        
        interaction_id=interaction_id,
        session_id=payload["session_id"],
        lead_id=payload["lead_id"],
        campaign_id=payload["campaign_id"],
        customer_id=payload["customer_id"],
        agent_id=payload.get("agent_id", "default-agent-id"), 
        # agent_id=payload["agent_id"],
        call_sid=payload.get("call_sid", ""),
        transcript_text=payload.get("transcript_text", ""),
        conversation_data=payload.get("conversation_data", {}),
        ended_at=datetime.fromisoformat(payload["ended_at"]),
        additional_data={}
    )

    processor = PostCallProcessor()
    result = await processor.process_post_call(ctx, single_prompt=True)

    # Downstream actions
    await trigger_signal_jobs(interaction_id, ctx.session_id, ctx.campaign_id, result.raw_response)
    await update_lead_stage(ctx.lead_id, interaction_id, result.call_stage)
    
    await metrics_tracker.track_processing_completed(interaction_id, result.tokens_used, result.latency_ms)