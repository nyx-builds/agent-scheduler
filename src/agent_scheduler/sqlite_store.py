"""SQLite persistence backend for agent-scheduler.

Provides a production-grade storage backend using SQLite,
complementing the JSON file-based store for lightweight use cases.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from agent_scheduler.models import Job, JobDependency, JobExecution
from agent_scheduler.webhook import Webhook, WebhookDelivery
from agent_scheduler.templates import JobTemplate
from agent_scheduler.store import JobStore


class SQLiteJobStore(JobStore):
    """SQLite-backed job storage for production use.

    Advantages over JSONJobStore:
    - Atomic transactions (no partial writes on crash)
    - Efficient queries with indexes
    - Better concurrency (WAL mode)
    - Scales to millions of records
    - Proper filtering at the database level
    """

    def __init__(self, db_path: Optional[str] = None) -> None:
        if db_path is None:
            data_dir = os.environ.get("SCHEDULER_DATA_DIR", str(Path.home() / ".agent-scheduler"))
            os.makedirs(data_dir, exist_ok=True)
            db_path = os.path.join(data_dir, "scheduler.db")
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """Get or create a database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def _init_db(self) -> None:
        """Initialize database tables and indexes."""
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                name TEXT NOT NULL,
                handler TEXT NOT NULL,
                status TEXT NOT NULL,
                priority TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                tags TEXT NOT NULL DEFAULT '[]',
                cron TEXT,
                next_run_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS executions (
                id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                data TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS dependencies (
                id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                depends_on_id TEXT NOT NULL,
                on_status TEXT NOT NULL,
                data TEXT NOT NULL,
                FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
                FOREIGN KEY (depends_on_id) REFERENCES jobs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS webhooks (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS webhook_deliveries (
                id TEXT PRIMARY KEY,
                webhook_id TEXT NOT NULL,
                data TEXT NOT NULL,
                event TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (webhook_id) REFERENCES webhooks(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS templates (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                name TEXT NOT NULL UNIQUE,
                category TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS api_keys (
                key TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                scopes TEXT NOT NULL DEFAULT '[]',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                last_used_at TEXT,
                request_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS rate_limits (
                id TEXT PRIMARY KEY,
                key_hash TEXT NOT NULL,
                window_start TEXT NOT NULL,
                request_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
            CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority);
            CREATE INDEX IF NOT EXISTS idx_jobs_next_run ON jobs(next_run_at);
            CREATE INDEX IF NOT EXISTS idx_jobs_name ON jobs(name);
            CREATE INDEX IF NOT EXISTS idx_executions_job_id ON executions(job_id);
            CREATE INDEX IF NOT EXISTS idx_executions_started ON executions(started_at);
            CREATE INDEX IF NOT EXISTS idx_dependencies_job_id ON dependencies(job_id);
            CREATE INDEX IF NOT EXISTS idx_dependencies_depends_on ON dependencies(depends_on_id);
            CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_webhook ON webhook_deliveries(webhook_id);
            CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_created ON webhook_deliveries(created_at);
            CREATE INDEX IF NOT EXISTS idx_templates_name ON templates(name);
            CREATE INDEX IF NOT EXISTS idx_templates_category ON templates(category);
            CREATE INDEX IF NOT EXISTS idx_rate_limits_key_window ON rate_limits(key_hash, window_start);
        """)
        conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ── Jobs ─────────────────────────────────────────────────

    def save_job(self, job: Job) -> None:
        conn = self._get_conn()
        data = job.model_dump(mode="json")
        conn.execute(
            """INSERT OR REPLACE INTO jobs (id, data, name, handler, status, priority, enabled, tags, cron, next_run_at, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                job.id,
                json.dumps(data, default=str),
                job.name,
                job.handler,
                job.status.value if hasattr(job.status, "value") else str(job.status),
                job.priority.value if hasattr(job.priority, "value") else str(job.priority),
                1 if job.enabled else 0,
                json.dumps(job.tags),
                job.cron,
                job.next_run_at.isoformat() if job.next_run_at else None,
                job.created_at.isoformat() if job.created_at else None,
                job.updated_at.isoformat() if job.updated_at else None,
            ),
        )
        conn.commit()

    def get_job(self, job_id: str) -> Optional[Job]:
        conn = self._get_conn()
        row = conn.execute("SELECT data FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        return Job.model_validate(json.loads(row["data"]))

    def get_job_by_name(self, name: str) -> Optional[Job]:
        conn = self._get_conn()
        row = conn.execute("SELECT data FROM jobs WHERE name = ?", (name,)).fetchone()
        if row is None:
            return None
        return Job.model_validate(json.loads(row["data"]))

    def delete_job(self, job_id: str) -> bool:
        conn = self._get_conn()
        cursor = conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        conn.commit()
        return cursor.rowcount > 0

    def list_jobs(
        self,
        enabled_only: bool = False,
        tag: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[Job]:
        conn = self._get_conn()
        query = "SELECT data FROM jobs WHERE 1=1"
        params: list[Any] = []

        if enabled_only:
            query += " AND enabled = 1"
        if status:
            query += " AND status = ?"
            params.append(status)
        if tag:
            query += " AND tags LIKE ?"
            params.append(f'%"{tag}"%')

        query += " ORDER BY created_at DESC"
        rows = conn.execute(query, params).fetchall()
        return [Job.model_validate(json.loads(row["data"])) for row in rows]

    # ── Executions ───────────────────────────────────────────

    def save_execution(self, execution: JobExecution) -> None:
        conn = self._get_conn()
        data = execution.model_dump(mode="json")
        conn.execute(
            """INSERT OR REPLACE INTO executions (id, job_id, data, status, started_at)
               VALUES (?, ?, ?, ?, ?)""",
            (
                execution.id,
                execution.job_id,
                json.dumps(data, default=str),
                execution.status.value if hasattr(execution.status, "value") else str(execution.status),
                execution.started_at.isoformat() if execution.started_at else None,
            ),
        )
        conn.commit()

    def get_executions(self, job_id: str, limit: int = 50, offset: int = 0) -> list[JobExecution]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT data FROM executions WHERE job_id = ? ORDER BY started_at DESC LIMIT ? OFFSET ?",
            (job_id, limit, offset),
        ).fetchall()
        return [JobExecution.model_validate(json.loads(row["data"])) for row in rows]

    def get_all_executions(self, limit: int = 100, offset: int = 0) -> list[JobExecution]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT data FROM executions ORDER BY started_at DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        return [JobExecution.model_validate(json.loads(row["data"])) for row in rows]

    def count_executions(self, job_id: Optional[str] = None) -> int:
        """Count total executions, optionally for a specific job."""
        conn = self._get_conn()
        if job_id:
            row = conn.execute("SELECT COUNT(*) as cnt FROM executions WHERE job_id = ?", (job_id,)).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) as cnt FROM executions").fetchone()
        return row["cnt"] if row else 0

    # ── Dependencies ─────────────────────────────────────────

    def save_dependency(self, dep: JobDependency) -> None:
        conn = self._get_conn()
        data = dep.model_dump(mode="json")
        conn.execute(
            """INSERT OR REPLACE INTO dependencies (id, job_id, depends_on_id, on_status, data)
               VALUES (?, ?, ?, ?, ?)""",
            (
                dep.id,
                dep.job_id,
                dep.depends_on_id,
                dep.on_status.value if hasattr(dep.on_status, "value") else str(dep.on_status),
                json.dumps(data, default=str),
            ),
        )
        conn.commit()

    def get_dependencies(self, job_id: str) -> list[JobDependency]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT data FROM dependencies WHERE job_id = ? OR depends_on_id = ?",
            (job_id, job_id),
        ).fetchall()
        return [JobDependency.model_validate(json.loads(row["data"])) for row in rows]

    def list_dependencies(self) -> list[JobDependency]:
        conn = self._get_conn()
        rows = conn.execute("SELECT data FROM dependencies").fetchall()
        return [JobDependency.model_validate(json.loads(row["data"])) for row in rows]

    def delete_dependency(self, dep_id: str) -> bool:
        conn = self._get_conn()
        cursor = conn.execute("DELETE FROM dependencies WHERE id = ?", (dep_id,))
        conn.commit()
        return cursor.rowcount > 0

    # ── Webhooks ─────────────────────────────────────────────

    def save_webhook(self, webhook: Webhook) -> None:
        conn = self._get_conn()
        data = webhook.model_dump(mode="json")
        conn.execute(
            """INSERT OR REPLACE INTO webhooks (id, data, name, url, enabled)
               VALUES (?, ?, ?, ?, ?)""",
            (
                webhook.id,
                json.dumps(data, default=str),
                webhook.name,
                webhook.url,
                1 if webhook.enabled else 0,
            ),
        )
        conn.commit()

    def get_webhook(self, webhook_id: str) -> Optional[Webhook]:
        conn = self._get_conn()
        row = conn.execute("SELECT data FROM webhooks WHERE id = ?", (webhook_id,)).fetchone()
        if row is None:
            return None
        return Webhook.model_validate(json.loads(row["data"]))

    def delete_webhook(self, webhook_id: str) -> bool:
        conn = self._get_conn()
        cursor = conn.execute("DELETE FROM webhooks WHERE id = ?", (webhook_id,))
        conn.commit()
        return cursor.rowcount > 0

    def list_webhooks(self, enabled_only: bool = False) -> list[Webhook]:
        conn = self._get_conn()
        query = "SELECT data FROM webhooks"
        if enabled_only:
            query += " WHERE enabled = 1"
        rows = conn.execute(query).fetchall()
        return [Webhook.model_validate(json.loads(row["data"])) for row in rows]

    # ── Webhook Deliveries ───────────────────────────────────

    def save_webhook_delivery(self, delivery: WebhookDelivery) -> None:
        conn = self._get_conn()
        data = delivery.model_dump(mode="json")
        conn.execute(
            """INSERT OR REPLACE INTO webhook_deliveries (id, webhook_id, data, event, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                delivery.id,
                delivery.webhook_id,
                json.dumps(data, default=str),
                delivery.event.value if hasattr(delivery.event, "value") else str(delivery.event),
                delivery.status.value if hasattr(delivery.status, "value") else str(delivery.status),
                delivery.created_at.isoformat() if delivery.created_at else None,
            ),
        )
        conn.commit()

    def get_webhook_delivery(self, delivery_id: str) -> Optional[WebhookDelivery]:
        conn = self._get_conn()
        row = conn.execute("SELECT data FROM webhook_deliveries WHERE id = ?", (delivery_id,)).fetchone()
        if row is None:
            return None
        return WebhookDelivery.model_validate(json.loads(row["data"]))

    def get_webhook_deliveries(self, webhook_id: Optional[str] = None, limit: int = 50, offset: int = 0) -> list[WebhookDelivery]:
        conn = self._get_conn()
        if webhook_id:
            rows = conn.execute(
                "SELECT data FROM webhook_deliveries WHERE webhook_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (webhook_id, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT data FROM webhook_deliveries ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [WebhookDelivery.model_validate(json.loads(row["data"])) for row in rows]

    # ── Templates ────────────────────────────────────────────

    def save_template(self, template: JobTemplate) -> None:
        conn = self._get_conn()
        data = template.model_dump(mode="json")
        conn.execute(
            """INSERT OR REPLACE INTO templates (id, data, name, category)
               VALUES (?, ?, ?, ?)""",
            (
                template.id,
                json.dumps(data, default=str),
                template.name,
                template.category.value if hasattr(template.category, "value") else str(template.category),
            ),
        )
        conn.commit()

    def get_template(self, template_id: str) -> Optional[JobTemplate]:
        conn = self._get_conn()
        row = conn.execute("SELECT data FROM templates WHERE id = ? OR name = ?", (template_id, template_id)).fetchone()
        if row is None:
            return None
        return JobTemplate.model_validate(json.loads(row["data"]))

    def delete_template(self, template_id: str) -> bool:
        conn = self._get_conn()
        cursor = conn.execute("DELETE FROM templates WHERE id = ? OR name = ?", (template_id, template_id))
        conn.commit()
        return cursor.rowcount > 0

    def list_templates(self, category: Optional[str] = None) -> list[JobTemplate]:
        conn = self._get_conn()
        if category:
            rows = conn.execute("SELECT data FROM templates WHERE category = ?", (category,)).fetchall()
        else:
            rows = conn.execute("SELECT data FROM templates").fetchall()
        return [JobTemplate.model_validate(json.loads(row["data"])) for row in rows]

    # ── API Keys ─────────────────────────────────────────────

    def save_api_key(self, key: str, name: str, scopes: list[str], enabled: bool = True) -> None:
        """Save an API key."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """INSERT OR REPLACE INTO api_keys (key, name, scopes, enabled, created_at, last_used_at, request_count)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (key, name, json.dumps(scopes), 1 if enabled else 0, now, None, 0),
        )
        conn.commit()

    def get_api_key(self, key: str) -> Optional[dict]:
        """Get an API key record. Returns dict with key, name, scopes, enabled, etc."""
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM api_keys WHERE key = ?", (key,)).fetchone()
        if row is None:
            return None
        return {
            "key": row["key"],
            "name": row["name"],
            "scopes": json.loads(row["scopes"]),
            "enabled": bool(row["enabled"]),
            "created_at": row["created_at"],
            "last_used_at": row["last_used_at"],
            "request_count": row["request_count"],
        }

    def list_api_keys(self) -> list[dict]:
        """List all API keys (key values are masked)."""
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM api_keys ORDER BY created_at DESC").fetchall()
        result = []
        for row in rows:
            result.append({
                "key": row["key"][:8] + "..." + row["key"][-4:] if len(row["key"]) > 12 else row["key"][:4] + "...",
                "name": row["name"],
                "scopes": json.loads(row["scopes"]),
                "enabled": bool(row["enabled"]),
                "created_at": row["created_at"],
                "last_used_at": row["last_used_at"],
                "request_count": row["request_count"],
            })
        return result

    def delete_api_key(self, key: str) -> bool:
        """Delete an API key."""
        conn = self._get_conn()
        cursor = conn.execute("DELETE FROM api_keys WHERE key = ?", (key,))
        conn.commit()
        return cursor.rowcount > 0

    def update_api_key_usage(self, key: str) -> None:
        """Update last_used_at and increment request_count for an API key."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE api_keys SET last_used_at = ?, request_count = request_count + 1 WHERE key = ?",
            (now, key),
        )
        conn.commit()

    def toggle_api_key(self, key: str, enabled: bool) -> bool:
        """Enable or disable an API key."""
        conn = self._get_conn()
        cursor = conn.execute("UPDATE api_keys SET enabled = ? WHERE key = ?", (1 if enabled else 0, key))
        conn.commit()
        return cursor.rowcount > 0

    # ── Rate Limiting ────────────────────────────────────────

    def check_rate_limit(self, key_hash: str, max_requests: int, window_seconds: int) -> tuple[bool, int]:
        """Check if a request is within rate limits.

        Returns (allowed, remaining_requests).
        Uses a sliding window approach.
        """
        conn = self._get_conn()
        now = datetime.now(timezone.utc)
        window_start = now.timestamp() - (now.timestamp() % window_seconds)
        window_start_str = datetime.fromtimestamp(window_start, tz=timezone.utc).isoformat()

        # Clean up old windows
        cutoff = datetime.fromtimestamp(now.timestamp() - window_seconds * 2, tz=timezone.utc).isoformat()
        conn.execute("DELETE FROM rate_limits WHERE window_start < ?", (cutoff,))

        # Get current window
        row = conn.execute(
            "SELECT request_count FROM rate_limits WHERE key_hash = ? AND window_start = ?",
            (key_hash, window_start_str),
        ).fetchone()

        current_count = row["request_count"] if row else 0

        if current_count >= max_requests:
            conn.commit()
            return (False, 0)

        # Increment
        if row:
            conn.execute(
                "UPDATE rate_limits SET request_count = request_count + 1 WHERE key_hash = ? AND window_start = ?",
                (key_hash, window_start_str),
            )
        else:
            import uuid
            conn.execute(
                "INSERT INTO rate_limits (id, key_hash, window_start, request_count) VALUES (?, ?, ?, 1)",
                (uuid.uuid4().hex[:12], key_hash, window_start_str),
            )
        conn.commit()
        return (True, max_requests - current_count - 1)
