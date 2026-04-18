"""Resumable progress tracking for the indexer job.

Persists per-(phase, indexer) progress to a JSON file under ``$WORKSPACE`` so
that a restarted job skips pairs already completed and re-runs the pair that
was in flight when the previous run died.

See /Users/rcourt/.claude/plans/agile-conjuring-walrus.md for design rationale.
"""

import datetime
import json
import logging
import os
from typing import Any, Dict, Optional


log = logging.getLogger(__name__)


SCHEMA_VERSION = 1

STATUS_PENDING = "pending"
STATUS_IN_PROGRESS = "in_progress"
STATUS_COMPLETED = "completed"
STATUS_EMPTY = "empty"
STATUS_FAILED = "failed"

RUN_IN_PROGRESS = "in_progress"
RUN_COMPLETED = "completed"
RUN_FAILED = "failed"


def _now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _resolve_path(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    env_path = os.environ.get("PROGRESS_FILE")
    if env_path:
        return env_path
    workspace = os.environ.get("WORKSPACE")
    if workspace:
        return os.path.join(workspace, "indexer_progress.json")
    return os.path.join(os.getcwd(), "indexer_progress.json")


def _pair_key(phase: str, indexer_name: str) -> str:
    return f"{phase}::{indexer_name}"


def log_progress(
    phase: str,
    ordinal: int,
    total: int,
    name: str,
    status: str,
    elapsed_seconds: Optional[float] = None,
    extra: Optional[str] = None,
) -> None:
    """Emit a single structured progress line to stdout via the logger."""
    parts = [
        "[progress]",
        f"phase={phase}",
        f"indexer={ordinal}/{total}",
        name,
        f"status={status}",
    ]
    if elapsed_seconds is not None:
        parts.append(f"elapsed={int(round(elapsed_seconds))}s")
    if extra:
        parts.append(extra)
    log.info(" ".join(parts))


class ProgressTracker:
    """Job-level progress tracker.

    State is flushed atomically after every mark. Safe to call ``load`` on a
    missing or corrupt file — both fall back to a fresh state with a warning.
    """

    def __init__(self, path: Optional[str] = None) -> None:
        self.path = _resolve_path(path)
        self.state: Dict[str, Any] = self._empty_state()

        if os.environ.get("PROGRESS_RESET") == "1":
            try:
                if os.path.exists(self.path):
                    os.remove(self.path)
                    log.info(f"[progress] PROGRESS_RESET=1 — removed existing progress file at {self.path}")
            except OSError as e:
                log.warning(f"[progress] failed to remove progress file for reset: {e}")

    @staticmethod
    def _empty_state() -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "run_started_at": _now_iso(),
            "run_finished_at": None,
            "run_status": RUN_IN_PROGRESS,
            "total_pairs": 0,
            "pairs": {},
        }

    def load(self) -> None:
        """Read state from disk if available. Reset any interrupted pairs."""
        log.info(f"[progress] using progress file: {self.path}")
        if not os.path.exists(self.path):
            log.info("[progress] no existing progress file — starting fresh run")
            self.state = self._empty_state()
            return

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            log.warning(f"[progress] progress file unreadable ({e}) — starting fresh run")
            self.state = self._empty_state()
            return

        if not isinstance(loaded, dict) or loaded.get("schema_version") != SCHEMA_VERSION:
            log.warning(
                f"[progress] progress file schema mismatch (got {loaded.get('schema_version') if isinstance(loaded, dict) else 'non-dict'}) — starting fresh run"
            )
            self.state = self._empty_state()
            return

        # If the previous run finished cleanly, tell the user how to force a re-run.
        if loaded.get("run_status") == RUN_COMPLETED:
            log.info(
                "[progress] previous run completed successfully — all pairs will be skipped. "
                "Set PROGRESS_RESET=1 (or delete the progress file) to force a fresh run."
            )

        # Previous run may have been mid-flight: reset any in-progress pairs to pending
        # so they re-run from scratch. partition_ids() will naturally skip work already
        # persisted to Solr.
        pairs = loaded.get("pairs") or {}
        for key, pair in pairs.items():
            if pair.get("status") == STATUS_IN_PROGRESS:
                log.info(f"[progress] resuming — resetting interrupted pair {key} to pending")
                pair["status"] = STATUS_PENDING
                pair["error"] = None

        # Resuming: the run is in progress again.
        loaded["run_status"] = RUN_IN_PROGRESS
        loaded["run_finished_at"] = None
        self.state = loaded

    def set_total_pairs(self, total: int) -> None:
        self.state["total_pairs"] = total
        self._flush()

    def should_skip(self, phase: str, indexer_name: str) -> bool:
        pair = self.state["pairs"].get(_pair_key(phase, indexer_name))
        if not pair:
            return False
        return pair.get("status") in (STATUS_COMPLETED, STATUS_EMPTY)

    def _upsert(
        self,
        phase: str,
        indexer_name: str,
        ordinal: int,
        status: str,
        duration_seconds: Optional[float] = None,
        error: Optional[str] = None,
    ) -> None:
        key = _pair_key(phase, indexer_name)
        now = _now_iso()
        pair = self.state["pairs"].get(key) or {
            "phase": phase,
            "indexer": indexer_name,
            "ordinal": ordinal,
            "status": STATUS_PENDING,
            "started_at": None,
            "finished_at": None,
            "duration_seconds": None,
            "error": None,
        }
        pair["ordinal"] = ordinal
        pair["status"] = status
        if status == STATUS_IN_PROGRESS:
            pair["started_at"] = now
            pair["finished_at"] = None
            pair["duration_seconds"] = None
            pair["error"] = None
        elif status in (STATUS_COMPLETED, STATUS_EMPTY, STATUS_FAILED):
            pair["finished_at"] = now
            if duration_seconds is not None:
                pair["duration_seconds"] = round(duration_seconds, 3)
            pair["error"] = error
        self.state["pairs"][key] = pair
        self._flush()

    def mark_in_progress(self, phase: str, indexer_name: str, ordinal: int, total: int) -> None:
        self._upsert(phase, indexer_name, ordinal, STATUS_IN_PROGRESS)
        log_progress(phase, ordinal, total, indexer_name, STATUS_IN_PROGRESS, elapsed_seconds=0)

    def mark_completed(self, phase: str, indexer_name: str, ordinal: int, total: int, duration_seconds: float) -> None:
        self._upsert(phase, indexer_name, ordinal, STATUS_COMPLETED, duration_seconds=duration_seconds)
        log_progress(phase, ordinal, total, indexer_name, STATUS_COMPLETED, elapsed_seconds=duration_seconds)

    def mark_empty(self, phase: str, indexer_name: str, ordinal: int, total: int, duration_seconds: float) -> None:
        self._upsert(phase, indexer_name, ordinal, STATUS_EMPTY, duration_seconds=duration_seconds)
        log_progress(phase, ordinal, total, indexer_name, STATUS_EMPTY, elapsed_seconds=duration_seconds)

    def mark_failed(
        self,
        phase: str,
        indexer_name: str,
        ordinal: int,
        total: int,
        exc: BaseException,
        duration_seconds: Optional[float] = None,
    ) -> None:
        error_str = f"{type(exc).__name__}: {exc}"
        self._upsert(phase, indexer_name, ordinal, STATUS_FAILED, duration_seconds=duration_seconds, error=error_str)
        log_progress(
            phase,
            ordinal,
            total,
            indexer_name,
            STATUS_FAILED,
            elapsed_seconds=duration_seconds,
            extra=f"error={error_str}",
        )

    def log_skip(self, phase: str, indexer_name: str, ordinal: int, total: int) -> None:
        log_progress(phase, ordinal, total, indexer_name, "skipped", extra="(already complete)")

    def finalize(self, total_elapsed_seconds: float) -> None:
        pairs = list(self.state["pairs"].values())
        total_pairs = self.state.get("total_pairs", len(pairs))
        completed_count = sum(1 for p in pairs if p.get("status") in (STATUS_COMPLETED, STATUS_EMPTY))
        # Run is only fully successful if every pair has a terminal status AND
        # we've seen at least total_pairs of them — a smaller count means
        # something was skipped or short-circuited before the loop finished.
        all_terminal = (
            completed_count == total_pairs
            and all(p.get("status") in (STATUS_COMPLETED, STATUS_EMPTY) for p in pairs)
        )

        self.state["run_status"] = RUN_COMPLETED if all_terminal else RUN_FAILED
        self.state["run_finished_at"] = _now_iso()
        self._flush()

        log.info(
            f"[progress] run {self.state['run_status']} — "
            f"{completed_count}/{total_pairs} pairs, "
            f"total_elapsed={int(round(total_elapsed_seconds))}s"
        )

    def _flush(self) -> None:
        tmp_path = self.path + ".tmp"
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self.path)
        except OSError as e:
            log.warning(f"[progress] failed to flush progress file to {self.path}: {e}")
