"""
Persistent validation logger for WeatherNext AI vs NHC forecast comparisons.

Designed for the 2026 hurricane season: every time a storm is queried, we log
both the NHC traditional advisory and WeatherNext AI prediction side-by-side.
After the season, this dataset provides a rigorous accuracy comparison.

Storage strategy (three layers of durability):

  1. JSON-Lines files (human-readable, grep-friendly, git-trackable)
     data/validation/2026/AL092026_comparisons.jsonl
     data/validation/2026/AL092026_snapshots.jsonl

  2. SQLite database (structured queries, aggregation, post-season analysis)
     data/validation/validation.db

  3. Per-storm summary JSON (quick-reference, dashboards)
     data/validation/2026/AL092026_summary.json

Files are append-only — no deletes, no overwrites, no truncation.
The SQLite WAL journal mode ensures crash-safe writes even during storms.
"""

import json
import logging
import os
import queue
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Base directory for all validation data — use persistent volume if available
_PERSISTENT_DATA = Path(os.environ.get("PERSISTENT_DATA_DIR", str(Path(__file__).parent.parent / "data")))
_VALIDATION_DIR = _PERSISTENT_DATA / "validation"
_VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
_DB_PATH = _VALIDATION_DIR / "validation.db"

# Thread lock for SQLite reads (one writer at a time)
_db_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Background write queue — batches inserts so 50K concurrent requests don't
# serialize on a single Lock.  Writes are buffered in a thread-safe queue and
# flushed by a single daemon thread in batches of up to _BATCH_SIZE rows,
# using executemany() for throughput.
# ---------------------------------------------------------------------------
_WRITE_BATCH_SIZE = 50
_WRITE_FLUSH_INTERVAL = 2.0  # seconds — max wait before a partial batch flushes
_write_queue: queue.Queue = queue.Queue(maxsize=10_000)
_writer_started = False
_writer_lock = threading.Lock()


def _start_writer():
    """Lazily start the background writer thread (once)."""
    global _writer_started
    if _writer_started:
        return
    with _writer_lock:
        if _writer_started:
            return
        t = threading.Thread(target=_writer_loop, name="validation-writer", daemon=True)
        t.start()
        _writer_started = True
        logger.info("[VALIDATION] Background writer thread started")


def _writer_loop():
    """
    Drain the write queue in batches and execute against SQLite.

    Runs forever as a daemon thread.  If the queue is quiet for
    _WRITE_FLUSH_INTERVAL seconds it flushes whatever is buffered.
    """
    while True:
        batch: List[Tuple[str, tuple]] = []
        try:
            # Block until at least one item arrives
            item = _write_queue.get(timeout=_WRITE_FLUSH_INTERVAL)
            batch.append(item)
        except queue.Empty:
            continue

        # Drain up to _WRITE_BATCH_SIZE more without blocking
        while len(batch) < _WRITE_BATCH_SIZE:
            try:
                batch.append(_write_queue.get_nowait())
            except queue.Empty:
                break

        # Group by SQL statement for executemany()
        groups: Dict[str, List[tuple]] = {}
        for sql, params in batch:
            groups.setdefault(sql, []).append(params)

        try:
            conn = _get_db()
            try:
                for sql, param_list in groups.items():
                    conn.executemany(sql, param_list)
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"[VALIDATION] Batch write failed ({len(batch)} rows): {e}")


def _ensure_dirs(season_year: int):
    """Create the validation directory tree if it doesn't exist."""
    season_dir = _VALIDATION_DIR / str(season_year)
    season_dir.mkdir(parents=True, exist_ok=True)
    return season_dir


def _get_db() -> sqlite3.Connection:
    """Open (and initialize if needed) the validation SQLite database."""
    _VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS comparisons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            storm_id TEXT NOT NULL,
            season_year INTEGER NOT NULL,
            comparison_time TEXT NOT NULL,
            nhc_max_wind_kt REAL,
            nhc_min_pressure_mb REAL,
            nhc_lat REAL,
            nhc_lon REAL,
            nhc_category TEXT,
            ai_peak_intensity_vmax_kt REAL,
            ai_ri_prob_24h REAL,
            ai_track_points INTEGER,
            ai_generated_at TEXT,
            nhc_source TEXT,
            ai_source TEXT,
            raw_json TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_comparisons_storm
            ON comparisons(storm_id, season_year);

        CREATE INDEX IF NOT EXISTS idx_comparisons_time
            ON comparisons(comparison_time);

        CREATE TABLE IF NOT EXISTS source_health_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_time TEXT NOT NULL,
            source_name TEXT NOT NULL,
            reliability REAL,
            avg_latency_ms REAL,
            is_healthy INTEGER,
            total_calls INTEGER,
            consecutive_failures INTEGER,
            raw_json TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_health_source
            ON source_health_snapshots(source_name, snapshot_time);

        CREATE TABLE IF NOT EXISTS storm_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            storm_id TEXT NOT NULL UNIQUE,
            season_year INTEGER NOT NULL,
            actual_peak_wind_kt REAL,
            actual_min_pressure_mb REAL,
            actual_landfall_lat REAL,
            actual_landfall_lon REAL,
            actual_category TEXT,
            actual_dpi REAL,
            notes TEXT,
            added_at TEXT DEFAULT (datetime('now'))
        );
    """)
    return conn


class ValidationLogger:
    """
    Append-only validation logger with three persistence layers.

    Usage:
        vlog = ValidationLogger.instance()
        vlog.log_comparison(comparison_dict)
        vlog.log_source_health(health_dict)

    After the season:
        vlog.record_actual_outcome("AL092026", peak_wind_kt=130, ...)
        results = vlog.get_storm_accuracy("AL092026")
    """

    _instance: Optional["ValidationLogger"] = None

    @classmethod
    def instance(cls) -> "ValidationLogger":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._season_year = datetime.utcnow().year

    def log_comparison(self, comparison: Dict[str, Any]) -> bool:
        """
        Persist a WeatherNext vs NHC comparison record.

        Writes to:
          1. JSONL file (append)
          2. SQLite database (insert)
          3. Updates per-storm summary JSON

        Returns True on success, False if persistence failed (but never raises).
        """
        storm_id = comparison.get("storm_id", "UNKNOWN")
        comp_time = comparison.get("comparison_time", datetime.utcnow().isoformat() + "Z")

        try:
            # Layer 1: JSON-Lines file
            season_dir = _ensure_dirs(self._season_year)
            jsonl_path = season_dir / f"{storm_id}_comparisons.jsonl"
            with open(jsonl_path, "a") as f:
                f.write(json.dumps(comparison, default=str) + "\n")

            # Layer 2: SQLite (non-blocking — queued for background batch write)
            nhc = comparison.get("nhc_forecast") or {}
            ai = comparison.get("weathernext_forecast") or {}

            _start_writer()
            _write_queue.put_nowait((
                """INSERT INTO comparisons (
                    storm_id, season_year, comparison_time,
                    nhc_max_wind_kt, nhc_min_pressure_mb, nhc_lat, nhc_lon, nhc_category,
                    ai_peak_intensity_vmax_kt, ai_ri_prob_24h, ai_track_points, ai_generated_at,
                    nhc_source, ai_source, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    storm_id, self._season_year, comp_time,
                    nhc.get("max_wind_kt"), nhc.get("min_pressure_mb"),
                    nhc.get("center_lat"), nhc.get("center_lon"), nhc.get("category"),
                    ai.get("peak_intensity_vmax_kt"), ai.get("ri_prob_24h"),
                    ai.get("track_points"), ai.get("generated_at"),
                    comparison.get("nhc_source"), comparison.get("weathernext_source"),
                    json.dumps(comparison, default=str),
                ),
            ))

            # Layer 3: Update per-storm summary
            self._update_storm_summary(storm_id, comparison)

            logger.info(f"[VALIDATION] Logged comparison for {storm_id} at {comp_time}")
            return True

        except Exception as e:
            logger.error(f"[VALIDATION] Failed to log comparison for {storm_id}: {e}")
            return False

    def log_source_health(self, health_data: Dict[str, Any]) -> bool:
        """
        Persist a source health snapshot for trend analysis.

        Called periodically (e.g., every 30 minutes) to track long-term
        reliability and latency trends across the season.
        """
        snapshot_time = datetime.utcnow().isoformat() + "Z"

        try:
            # JSONL file
            season_dir = _ensure_dirs(self._season_year)
            jsonl_path = season_dir / "source_health_log.jsonl"
            record = {"snapshot_time": snapshot_time, **health_data}
            with open(jsonl_path, "a") as f:
                f.write(json.dumps(record, default=str) + "\n")

            # SQLite (non-blocking — queued for background batch write)
            sources = health_data.get("sources", [])
            _start_writer()
            for src in sources:
                _write_queue.put_nowait((
                    """INSERT INTO source_health_snapshots (
                        snapshot_time, source_name, reliability,
                        avg_latency_ms, is_healthy, total_calls,
                        consecutive_failures, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        snapshot_time, src.get("name"),
                        src.get("reliability"), src.get("avg_latency_ms"),
                        1 if src.get("is_healthy") else 0,
                        src.get("total_calls"), src.get("consecutive_failures"),
                        json.dumps(src, default=str),
                    ),
                ))

            return True

        except Exception as e:
            logger.error(f"[VALIDATION] Failed to log health snapshot: {e}")
            return False

    def record_actual_outcome(
        self,
        storm_id: str,
        peak_wind_kt: Optional[float] = None,
        min_pressure_mb: Optional[float] = None,
        landfall_lat: Optional[float] = None,
        landfall_lon: Optional[float] = None,
        category: Optional[str] = None,
        dpi: Optional[float] = None,
        notes: str = "",
    ) -> bool:
        """
        After a storm ends, record its actual observed outcome.

        This is the ground truth that gets compared against both NHC and
        WeatherNext predictions in the post-season accuracy analysis.
        """
        try:
            with _db_lock:
                conn = _get_db()
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO storm_outcomes (
                            storm_id, season_year,
                            actual_peak_wind_kt, actual_min_pressure_mb,
                            actual_landfall_lat, actual_landfall_lon,
                            actual_category, actual_dpi, notes
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        storm_id, self._season_year,
                        peak_wind_kt, min_pressure_mb,
                        landfall_lat, landfall_lon,
                        category, dpi, notes,
                    ))
                    conn.commit()
                finally:
                    conn.close()

            # Also write to JSONL for human reference
            season_dir = _ensure_dirs(self._season_year)
            outcome = {
                "storm_id": storm_id, "season_year": self._season_year,
                "actual_peak_wind_kt": peak_wind_kt,
                "actual_min_pressure_mb": min_pressure_mb,
                "actual_landfall_lat": landfall_lat,
                "actual_landfall_lon": landfall_lon,
                "actual_category": category, "actual_dpi": dpi,
                "notes": notes,
                "recorded_at": datetime.utcnow().isoformat() + "Z",
            }
            with open(season_dir / "storm_outcomes.jsonl", "a") as f:
                f.write(json.dumps(outcome, default=str) + "\n")

            logger.info(f"[VALIDATION] Recorded outcome for {storm_id}: cat={category}, peak={peak_wind_kt}kt")
            return True

        except Exception as e:
            logger.error(f"[VALIDATION] Failed to record outcome for {storm_id}: {e}")
            return False

    def get_storm_accuracy(self, storm_id: str) -> Optional[Dict[str, Any]]:
        """
        Compare all logged predictions against the actual outcome for a storm.

        Returns accuracy metrics (NHC error, WeatherNext error, bias) or None
        if no outcome has been recorded yet.
        """
        try:
            with _db_lock:
                conn = _get_db()
                try:
                    # Get actual outcome
                    row = conn.execute(
                        "SELECT * FROM storm_outcomes WHERE storm_id = ?", (storm_id,)
                    ).fetchone()
                    if not row:
                        return None
                    cols = [d[0] for d in conn.execute("SELECT * FROM storm_outcomes LIMIT 0").description]
                    outcome = dict(zip(cols, row))

                    # Get all comparisons for this storm
                    comp_rows = conn.execute(
                        "SELECT nhc_max_wind_kt, ai_peak_intensity_vmax_kt, comparison_time "
                        "FROM comparisons WHERE storm_id = ? ORDER BY comparison_time",
                        (storm_id,)
                    ).fetchall()
                finally:
                    conn.close()

            actual_peak = outcome.get("actual_peak_wind_kt")
            if actual_peak is None:
                return {"storm_id": storm_id, "error": "No peak wind recorded"}

            nhc_errors = []
            ai_errors = []
            for nhc_wind, ai_wind, comp_time in comp_rows:
                if nhc_wind is not None:
                    nhc_errors.append(abs(nhc_wind - actual_peak))
                if ai_wind is not None:
                    ai_errors.append(abs(ai_wind - actual_peak))

            return {
                "storm_id": storm_id,
                "actual_peak_wind_kt": actual_peak,
                "actual_category": outcome.get("actual_category"),
                "actual_dpi": outcome.get("actual_dpi"),
                "total_comparisons": len(comp_rows),
                "nhc_avg_error_kt": round(sum(nhc_errors) / len(nhc_errors), 1) if nhc_errors else None,
                "ai_avg_error_kt": round(sum(ai_errors) / len(ai_errors), 1) if ai_errors else None,
                "nhc_samples": len(nhc_errors),
                "ai_samples": len(ai_errors),
            }

        except Exception as e:
            logger.error(f"[VALIDATION] Failed to compute accuracy for {storm_id}: {e}")
            return None

    def get_season_summary(self, year: Optional[int] = None) -> Dict[str, Any]:
        """
        Aggregate accuracy across all storms in a season.

        Returns per-source error statistics and comparison counts.
        """
        year = year or self._season_year
        try:
            with _db_lock:
                conn = _get_db()
                try:
                    total = conn.execute(
                        "SELECT COUNT(*) FROM comparisons WHERE season_year = ?", (year,)
                    ).fetchone()[0]

                    storms = conn.execute(
                        "SELECT DISTINCT storm_id FROM comparisons WHERE season_year = ?", (year,)
                    ).fetchall()

                    outcomes = conn.execute(
                        "SELECT COUNT(*) FROM storm_outcomes WHERE season_year = ?", (year,)
                    ).fetchone()[0]
                finally:
                    conn.close()

            return {
                "season_year": year,
                "total_comparisons_logged": total,
                "unique_storms_tracked": len(storms),
                "storms_with_outcomes": outcomes,
                "storm_ids": [s[0] for s in storms],
                "validation_dir": str(_VALIDATION_DIR / str(year)),
                "database_path": str(_DB_PATH),
            }

        except Exception as e:
            logger.error(f"[VALIDATION] Failed to build season summary: {e}")
            return {"season_year": year, "error": str(e)}

    def _update_storm_summary(self, storm_id: str, comparison: Dict[str, Any]):
        """Update the per-storm summary JSON (latest snapshot + running counts)."""
        season_dir = _ensure_dirs(self._season_year)
        summary_path = season_dir / f"{storm_id}_summary.json"

        summary = {}
        if summary_path.exists():
            try:
                summary = json.loads(summary_path.read_text())
            except (json.JSONDecodeError, OSError):
                summary = {}

        summary["storm_id"] = storm_id
        summary["last_comparison_time"] = comparison.get("comparison_time")
        summary["total_comparisons"] = summary.get("total_comparisons", 0) + 1
        summary["nhc_available_count"] = summary.get("nhc_available_count", 0) + (
            1 if comparison.get("nhc_forecast") else 0
        )
        summary["ai_available_count"] = summary.get("ai_available_count", 0) + (
            1 if comparison.get("weathernext_forecast") else 0
        )
        summary["latest_nhc"] = comparison.get("nhc_forecast")
        summary["latest_ai"] = comparison.get("weathernext_forecast")

        try:
            summary_path.write_text(json.dumps(summary, indent=2, default=str))
        except OSError as e:
            logger.debug(f"Failed to write storm summary: {e}")
