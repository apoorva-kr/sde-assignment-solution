# Post-Call Processing Pipeline — Design Document

**Author:** Apoorva
**Date:** June 27, 2026

---

## 1. Assumptions
1. **Recording Latency:** Telephony providers exhibit variable delivery latency; the current 45s hard-wait is insufficient.
2. **LLM Limits:** The provider’s 429 rate limit is a hard constraint that must be managed via client-side throttling.
3. **Business Logic:** Call outcomes are not created equal; "Rebook" and "Escalation" outcomes require immediate processing, while "Not Interested" outcomes are batch-deferrable.

## 2. Problem Diagnosis
The existing system is brittle due to:
* **Silent Data Loss:** The `asyncio.sleep(45s)` strategy discards data if the recording arrives at 46 seconds.
* **Cascading Failure:** The blunt circuit breaker stops all operations when the LLM is under load, resulting in total business stoppage.
* **Non-Durable Queuing:** Relying solely on Celery/Redis without database-backed state causes task loss during infrastructure restarts.

## 3. Architecture Overview


The redesigned pipeline shifts to a **Database-as-Source-of-Truth** model:
1. **Ingest:** Webhooks write raw events to Postgres with a `PENDING` status.
2. **Orchestration:** Celery tasks are status-aware, allowing for restarts without task loss.
3. **Throttling:** A Redis-based Token Bucket acts as a gatekeeper before every LLM request.

## 4. Rate Limit Management
* **Tracking:** We utilize a Redis Token Bucket (`rate_limit:{customer_id}`) to track consumption.
* **Throttling:** Before any LLM call, workers request tokens. If denied, the task raises `self.retry(countdown=60)`, effectively moving the task to a wait-queue rather than failing.
* **Recovery:** Tasks are automatically re-queued by Celery, ensuring they are retried once the token bucket refills.

## 5. Per-Customer Token Budgeting
* **Isolation:** Each customer has a unique Redis bucket, preventing "noisy neighbor" scenarios where one high-volume customer exhausts the global limit.
* **Headroom:** Unused tokens are treated as a shared pool to maximize total system throughput.

## 6. Differentiated Processing
We implement a **Priority Lane** system:
* **High-Priority (Hot):** Rebooks/Escalations are routed to a `priority_queue` with higher worker allocation.
* **Low-Priority (Cold):** General queries are routed to the `default_queue` and are processed using only available surplus tokens.

## 7. Recording Pipeline
Replaced blocking sleeps with an **Asynchronous Poller**:
* The poller checks for the recording and, if missing, schedules a follow-up check using `apply_async(countdown=...)` with exponential backoff.
* **Visibility:** If the recording is not found after 5 attempts, a structured log event is emitted for the on-call engineer.

## 8. Reliability & Durability
* **State Persistence:** All task progress is recorded in the `interactions` table.
* **Idempotency:** The LLM processor is designed to be idempotent; if a task restarts, it simply re-evaluates or updates the existing interaction metadata.

## 9. Auditability & Observability
* **Logging:** Every log contains `interaction_id` and `customer_id`.
* **Alerting:** We monitor the `error_log` JSONB column. Any entry here triggers an automated alert to the on-call engineer.

## 10. Data Model
```sql
-- Schema adjustments for durability
ALTER TABLE interactions 
ADD COLUMN processing_priority INTEGER DEFAULT 0,
ADD COLUMN status VARCHAR(20) DEFAULT 'PENDING',
ADD COLUMN error_log JSONB DEFAULT '[]';


11. Security
> At Rest: Sensitive transcripts are stored in JSONB columns. We recommend enabling TDE (Transparent Data Encryption) at the database level.

> In Transit: All external communication is enforced over TLS 1.3

12. API Interface

The endpoint POST /session/{sid}/interaction/{iid}/end now returns a 202 Accepted status immediately, confirming that the request is durable and queued, rather than waiting for the entire processing chain to complete.

## 13. Trade-offs & Alternatives Considered

| Option | Rejected | Reason |
|--------|----------|--------|
| Rate Limit Proxy | Too much infrastructure overhead | Redis Token Bucket is native to our stack |
| SQS | Required cloud-specific infra | Celery+Redis provides portability |

## 14. Known Weaknesses
* **Hinglish Nuance:** Keyword-based priority classification may misclassify complex, informal language intents.

## 15. What I Would Do With More Time
1. Implement a real-time dashboard for "Queue Depth" visualization.
2. Develop a UI for customers to configure their own priority rules.




 1. Design Decisions
- **Error Handling:** Implemented explicit `KeyError` checks and retry logic to prevent silent task failures.
- **Recording Pipeline:** Switched from `asyncio.sleep` to a recursive task pattern to allow for scalable, non-blocking polling.

 2. Assumptions
- **API Connectivity:** Assumed 401 Unauthorized errors observed in logs were environment-specific; logic is built to handle authentication dynamically.
- **Task Durability:** Assumed that using Celery's native retry mechanisms satisfies the "no data loss" constraint.

3. Rate Limit Strategy
- Implemented a basic retry-backoff in the Celery task to handle LLM 429 responses, ensuring the system recovers gracefully during high load.