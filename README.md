# Voice AI Post-Call Processing Pipeline

## Overview
A production-ready pipeline for processing voice bot interactions, featuring asynchronous recording polling, persistent task state management, and LLM rate-limit awareness.

## System Features
- **Persistent Task Queue:** Uses Celery + Redis for reliable background processing.
- **Resilient Polling:** Replaced blocking sleep cycles with recursive, backoff-enabled polling tasks to ensure recording retrieval.
- **Rate-Limit Aware:** Orchestrates LLM analysis to respect API throughput constraints.

## Setup
1. **Infrastructure:** `docker-compose up -d` (Postgres & Redis).
2. **Environment:** Copy `.env.example` to `.env` and configure your API keys.
3. **Run:** `celery -A src.tasks.celery_app worker --loglevel=info --pool=solo --include=src.tasks.celery_tasks`