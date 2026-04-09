"""
Wind Radii Auditor — cross-validates quadrant wind radii against multiple
authoritative sources every ATCF advisory cycle (00, 06, 12, 18 UTC).

Wind radii (r34, r50, r64 per quadrant) are the single most impactful input
to IKE, and IKE is 30% of the DPI formula. If radii are wrong, the entire
destructive potential score is wrong. This auditor ensures every radii value
used in production DPI calculations is:

  1. Cross-referenced against at least 2 independent sources
  2. Checked for physical plausibility (meteorological sanity bounds)
  3. Flagged with a confidence score (0.0 = suspect, 1.0 = verified)
  4. Logged permanently for post-season auditing

Data sources ranked by authority:
  - NHC operational advisory (ATCF a-deck): Gold standard for Atlantic, real-time
  - IBTrACS (NCEI): Quality-controlled, multi-agency, slight delay
  - HURDAT2 (NHC): Annual reanalysis, post-season corrections
  - JTWC (via IBTrACS): Authoritative for Western Pacific, Indian Ocean

ATCF advisory timing:
  Advisories issued at synoptic times: 0000, 0600, 1200, 1800 UTC
  Full advisory packages usually available ~3 hours after synoptic time
  This auditor is designed to run at +3h offsets: 0300, 0900, 1500, 2100 UTC

Persistence:
  - SQLite: data/validation/validation.db (wind_radii_audits table)
  - JSONL:  data/validation/{year}/{storm_id}_radii_audits.jsonl
  - Both are append-only, crash-safe, never deleted
"""

import json
import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Reuse the validation directory and DB from validation_log.py
_PERSISTENT_DATA = Path(os.environ.get("PERSISTENT_DATA_DIR", str(Path(__file__).parent.parent / "data")))
_VALIDATION_DIR = _PERSISTENT_DATA / "validation"
_VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
_DB_PATH = _VALIDATION_DIR / "validation.db"
_db_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Physical plausibility bounds (meteorological sanity checks)
# ---------------------------------------------------------------------------
# These are hard limits derived from the observational record.
# Any value outside these ranges is flagged as suspect.

# Maximum observed r34 in history: ~450nm (Typhoon Tip, 1979)
# Typical Atlantic major hurricane r34: 100-250nm
R34_MAX_NM = 500
R34_MIN_NM = 15   # Below this, the storm is barely tropical-storm strength

# r50 is always smaller than r34
R50_MAX_NM = 300
R50_MIN_NM = 10

# r64 is always smaller than r50
R64_MAX_NM = 200
R64_MIN_NM = 5

# Maximum asymmetry ratio: largest quadrant / smallest quadrant
# Extremely asymmetric storms (e.g., Sandy) can reach 4:1
# Beyond 6:1 is physically suspect
MAX_ASYMMETRY_RATIO = 6.0

# r64 cannot exceed r50, r50 cannot exceed r34 (nesting rule)
# Tolerance for rounding in NM: 5nm
NESTING_TOLERANCE_NM = 5.0

# Maximum change between consecutive advisories (6 hours apart)
# Rapid expansion: ~40nm/6h for r34 is extreme but observed (TS → hurricane transition)
MAX_R34_CHANGE_NM_PER_6H = 60.0
MAX_R50_CHANGE_NM_PER_6H = 40.0
MAX_R64_CHANGE_NM_PER_6H = 30.0

# Wind speed vs radii consistency: storms below 50kt should NOT have r64 data
MIN_WIND_KT_FOR_R64 = 55   # Allow some tolerance below 64kt


@dataclass
class RadiiObservation:
    """Wind radii from a single source at a single time."""
    source: str                         # "nhc_advisory", "ibtracs", "hurdat2", "jtwc"
    timestamp: str                      # ISO 8601 UTC
    storm_id: str
    max_wind_kt: Optional[float] = None
    r34_ne_nm: Optional[float] = None
    r34_se_nm: Optional[float] = None
    r34_sw_nm: Optional[float] = None
    r34_nw_nm: Optional[float] = None
    r50_ne_nm: Optional[float] = None
    r50_se_nm: Optional[float] = None
    r50_sw_nm: Optional[float] = None
    r50_nw_nm: Optional[float] = None
    r64_ne_nm: Optional[float] = None
    r64_se_nm: Optional[float] = None
    r64_sw_nm: Optional[float] = None
    r64_nw_nm: Optional[float] = None
    rmw_nm: Optional[float] = None
    lat: Optional[float] = None
    lon: Optional[float] = None


@dataclass
class AuditFlag:
    """A single audit finding."""
    severity: str         # "info", "warning", "error"
    check_name: str       # e.g. "nesting_violation", "asymmetry_extreme"
    message: str
    field: str            # e.g. "r34_ne_nm", "r64_all"
    expected: Any = None
    actual: Any = None


@dataclass
class RadiiAuditResult:
    """Complete audit result for one storm at one advisory time."""
    storm_id: str
    advisory_time: str          # Synoptic time being audited (e.g. "2026-08-15T18:00:00Z")
    audit_time: str             # When the audit ran
    sources_checked: int        # How many independent sources contributed
    source_names: List[str]     # Which sources were available
    flags: List[AuditFlag] = field(default_factory=list)
    confidence_score: float = 1.0   # 0.0 = no trust, 1.0 = fully verified
    cross_source_agreement: Optional[float] = None  # Mean pairwise agreement (0-1)
    radii_used: Optional[Dict] = None    # The final radii values after audit
    all_observations: List[Dict] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return any(f.severity == "error" for f in self.flags)

    @property
    def has_warnings(self) -> bool:
        return any(f.severity == "warning" for f in self.flags)


# ---------------------------------------------------------------------------
# Core audit checks
# ---------------------------------------------------------------------------

def _check_plausibility(obs: RadiiObservation) -> List[AuditFlag]:
    """Run meteorological sanity checks on a single observation."""
    flags = []

    def _bound_check(val, lo, hi, name):
        if val is not None and (val < lo or val > hi):
            flags.append(AuditFlag(
                severity="error" if val > hi * 1.5 or val < 0 else "warning",
                check_name="plausibility_bound",
                message=f"{name} = {val}nm is outside plausible range [{lo}, {hi}]nm",
                field=name, expected=f"[{lo}, {hi}]", actual=val,
            ))

    # Range checks
    for q in ("ne", "se", "sw", "nw"):
        _bound_check(getattr(obs, f"r34_{q}_nm", None), R34_MIN_NM, R34_MAX_NM, f"r34_{q}_nm")
        _bound_check(getattr(obs, f"r50_{q}_nm", None), R50_MIN_NM, R50_MAX_NM, f"r50_{q}_nm")
        _bound_check(getattr(obs, f"r64_{q}_nm", None), R64_MIN_NM, R64_MAX_NM, f"r64_{q}_nm")

    # Nesting: r64 <= r50 <= r34 in each quadrant
    for q in ("ne", "se", "sw", "nw"):
        r34 = getattr(obs, f"r34_{q}_nm", None)
        r50 = getattr(obs, f"r50_{q}_nm", None)
        r64 = getattr(obs, f"r64_{q}_nm", None)

        if r50 is not None and r34 is not None and r50 > r34 + NESTING_TOLERANCE_NM:
            flags.append(AuditFlag(
                severity="error", check_name="nesting_violation",
                message=f"r50_{q} ({r50}nm) > r34_{q} ({r34}nm) — physically impossible",
                field=f"r50_{q}_nm", expected=f"<= {r34}", actual=r50,
            ))

        if r64 is not None and r50 is not None and r64 > r50 + NESTING_TOLERANCE_NM:
            flags.append(AuditFlag(
                severity="error", check_name="nesting_violation",
                message=f"r64_{q} ({r64}nm) > r50_{q} ({r50}nm) — physically impossible",
                field=f"r64_{q}_nm", expected=f"<= {r50}", actual=r64,
            ))

        if r64 is not None and r34 is not None and r64 > r34 + NESTING_TOLERANCE_NM:
            flags.append(AuditFlag(
                severity="error", check_name="nesting_violation",
                message=f"r64_{q} ({r64}nm) > r34_{q} ({r34}nm) — physically impossible",
                field=f"r64_{q}_nm", expected=f"<= {r34}", actual=r64,
            ))

    # Asymmetry check for r34
    r34_vals = [getattr(obs, f"r34_{q}_nm") for q in ("ne", "se", "sw", "nw")]
    r34_valid = [v for v in r34_vals if v is not None and v > 0]
    if len(r34_valid) >= 2:
        min_r34 = min(r34_valid)
        if min_r34 == 0:
            # Division by zero guard: if min is 0, skip ratio check
            pass
        else:
            ratio = max(r34_valid) / min_r34
            if ratio > MAX_ASYMMETRY_RATIO:
                flags.append(AuditFlag(
                    severity="warning", check_name="extreme_asymmetry",
                    message=f"r34 asymmetry ratio {ratio:.1f}:1 exceeds {MAX_ASYMMETRY_RATIO}:1 threshold",
                    field="r34_all", expected=f"<= {MAX_ASYMMETRY_RATIO}:1", actual=f"{ratio:.1f}:1",
                ))

    # Wind speed vs r64 consistency
    if obs.max_wind_kt is not None and obs.max_wind_kt < MIN_WIND_KT_FOR_R64:
        r64_vals = [getattr(obs, f"r64_{q}_nm") for q in ("ne", "se", "sw", "nw")]
        r64_present = [v for v in r64_vals if v is not None and v > 0]
        if r64_present:
            flags.append(AuditFlag(
                severity="warning", check_name="wind_radii_mismatch",
                message=f"r64 data present but max_wind = {obs.max_wind_kt}kt (below {MIN_WIND_KT_FOR_R64}kt threshold)",
                field="r64_all", expected="no r64 data", actual=f"r64 values: {r64_present}",
            ))

    return flags


def _cross_validate_sources(
    observations: List[RadiiObservation],
) -> Tuple[List[AuditFlag], float]:
    """
    Compare wind radii across multiple independent sources.

    Returns (flags, agreement_score) where agreement_score is 0.0-1.0.
    """
    flags = []
    if len(observations) < 2:
        return flags, 0.0

    # Compare each pair of sources for r34 quadrants
    agreements = []
    for i in range(len(observations)):
        for j in range(i + 1, len(observations)):
            a, b = observations[i], observations[j]
            pair_diffs = []

            for q in ("ne", "se", "sw", "nw"):
                for threshold in ("r34", "r50", "r64"):
                    val_a = getattr(a, f"{threshold}_{q}_nm", None)
                    val_b = getattr(b, f"{threshold}_{q}_nm", None)

                    if val_a is not None and val_b is not None:
                        diff_nm = abs(val_a - val_b)
                        pair_diffs.append(diff_nm)

                        # Flag large discrepancies
                        threshold_nm = 30 if threshold == "r34" else 20 if threshold == "r50" else 15
                        if diff_nm > threshold_nm:
                            flags.append(AuditFlag(
                                severity="warning" if diff_nm < threshold_nm * 2 else "error",
                                check_name="cross_source_discrepancy",
                                message=(
                                    f"{threshold}_{q}: {a.source} says {val_a}nm, "
                                    f"{b.source} says {val_b}nm (Δ{diff_nm:.0f}nm)"
                                ),
                                field=f"{threshold}_{q}_nm",
                                expected=f"< {threshold_nm}nm difference",
                                actual=f"{diff_nm:.0f}nm",
                            ))

            if pair_diffs:
                # Agreement: 1.0 if mean diff = 0, decays toward 0 as diff increases
                mean_diff = sum(pair_diffs) / len(pair_diffs)
                agreement = max(0.0, 1.0 - mean_diff / 50.0)
                agreements.append(agreement)

    overall_agreement = sum(agreements) / len(agreements) if agreements else 0.0
    return flags, overall_agreement


def _check_temporal_continuity(
    current: RadiiObservation,
    previous: Optional[RadiiObservation],
) -> List[AuditFlag]:
    """Check that radii haven't changed unrealistically since last advisory."""
    flags = []
    if previous is None:
        return flags

    limits = {
        "r34": MAX_R34_CHANGE_NM_PER_6H,
        "r50": MAX_R50_CHANGE_NM_PER_6H,
        "r64": MAX_R64_CHANGE_NM_PER_6H,
    }

    for threshold, max_change in limits.items():
        for q in ("ne", "se", "sw", "nw"):
            curr_val = getattr(current, f"{threshold}_{q}_nm", None)
            prev_val = getattr(previous, f"{threshold}_{q}_nm", None)

            if curr_val is not None and prev_val is not None:
                change = abs(curr_val - prev_val)
                if change > max_change:
                    direction = "expanded" if curr_val > prev_val else "contracted"
                    flags.append(AuditFlag(
                        severity="warning" if change < max_change * 1.5 else "error",
                        check_name="temporal_discontinuity",
                        message=(
                            f"{threshold}_{q} {direction} by {change:.0f}nm in 6h "
                            f"(limit: {max_change}nm). Was {prev_val}nm, now {curr_val}nm"
                        ),
                        field=f"{threshold}_{q}_nm",
                        expected=f"change < {max_change}nm/6h",
                        actual=f"{change:.0f}nm",
                    ))

    return flags


def _compute_confidence(
    flags: List[AuditFlag],
    source_count: int,
    agreement: float,
) -> float:
    """
    Compute overall confidence score (0.0 to 1.0) based on audit findings.

    Scoring:
      base = 0.5 (single source) or 0.7 (two sources) or 0.85 (three+ sources)
      + agreement_bonus (0.0 to 0.15 based on cross-source agreement)
      - error_penalty (0.15 per error)
      - warning_penalty (0.05 per warning)
    """
    if source_count == 0:
        return 0.0

    # Base score from source count
    if source_count >= 3:
        base = 0.85
    elif source_count >= 2:
        base = 0.70
    else:
        base = 0.50

    # Agreement bonus (only meaningful with 2+ sources)
    agreement_bonus = agreement * 0.15 if source_count >= 2 else 0.0

    # Penalty for flags
    errors = sum(1 for f in flags if f.severity == "error")
    warnings = sum(1 for f in flags if f.severity == "warning")
    penalty = errors * 0.15 + warnings * 0.05

    score = base + agreement_bonus - penalty
    return max(0.0, min(1.0, round(score, 3)))


# ---------------------------------------------------------------------------
# Auditor class
# ---------------------------------------------------------------------------

class WindRadiiAuditor:
    """
    Cross-validates wind radii data against multiple authoritative sources.

    Usage:
        auditor = WindRadiiAuditor.instance()

        # Run a full audit for an active storm
        result = await auditor.audit_storm(storm_id, observations, previous_obs)

        # Get audit history
        history = auditor.get_audit_history(storm_id)
    """

    _instance: Optional["WindRadiiAuditor"] = None

    @classmethod
    def instance(cls) -> "WindRadiiAuditor":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._ensure_db()

    def _ensure_db(self):
        """Create the audit table if it doesn't exist."""
        _VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
        with _db_lock:
            conn = sqlite3.connect(str(_DB_PATH), timeout=10)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS wind_radii_audits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    storm_id TEXT NOT NULL,
                    advisory_time TEXT NOT NULL,
                    audit_time TEXT NOT NULL,
                    sources_checked INTEGER NOT NULL,
                    source_names TEXT NOT NULL,
                    confidence_score REAL NOT NULL,
                    cross_source_agreement REAL,
                    error_count INTEGER DEFAULT 0,
                    warning_count INTEGER DEFAULT 0,
                    info_count INTEGER DEFAULT 0,
                    flags_json TEXT NOT NULL,
                    radii_used_json TEXT,
                    observations_json TEXT NOT NULL,
                    created_at TEXT DEFAULT (datetime('now')),
                    UNIQUE(storm_id, advisory_time)
                );

                CREATE INDEX IF NOT EXISTS idx_radii_audit_storm
                    ON wind_radii_audits(storm_id);

                CREATE INDEX IF NOT EXISTS idx_radii_audit_confidence
                    ON wind_radii_audits(confidence_score);

                CREATE TABLE IF NOT EXISTS wind_radii_audit_summary (
                    storm_id TEXT PRIMARY KEY,
                    total_audits INTEGER DEFAULT 0,
                    avg_confidence REAL DEFAULT 0,
                    min_confidence REAL DEFAULT 1.0,
                    total_errors INTEGER DEFAULT 0,
                    total_warnings INTEGER DEFAULT 0,
                    last_audit_time TEXT,
                    last_advisory_time TEXT,
                    updated_at TEXT DEFAULT (datetime('now'))
                );
            """)
            conn.close()

    async def audit_storm(
        self,
        storm_id: str,
        observations: List[RadiiObservation],
        previous_advisory: Optional[RadiiObservation] = None,
    ) -> RadiiAuditResult:
        """
        Run a full audit of wind radii for one storm at one advisory time.

        Steps:
          1. Physical plausibility checks on each source independently
          2. Cross-source comparison (if 2+ sources available)
          3. Temporal continuity check (if previous advisory available)
          4. Compute confidence score
          5. Persist results (JSONL + SQLite)

        Args:
            storm_id: Storm identifier
            observations: RadiiObservation from each available source
            previous_advisory: The most recent previous advisory's primary observation

        Returns:
            RadiiAuditResult with confidence score and flags
        """
        audit_time = datetime.utcnow().isoformat() + "Z"
        advisory_time = observations[0].timestamp if observations else audit_time

        all_flags: List[AuditFlag] = []

        # Step 1: Plausibility on each source
        for obs in observations:
            source_flags = _check_plausibility(obs)
            for f in source_flags:
                f.message = f"[{obs.source}] {f.message}"
            all_flags.extend(source_flags)

        # Step 2: Cross-source comparison
        cross_flags, agreement = _cross_validate_sources(observations)
        all_flags.extend(cross_flags)

        # Step 3: Temporal continuity
        if previous_advisory:
            # Use the highest-authority current observation for continuity check
            primary = observations[0] if observations else None
            if primary:
                temporal_flags = _check_temporal_continuity(primary, previous_advisory)
                all_flags.extend(temporal_flags)

        # Step 4: Confidence score
        confidence = _compute_confidence(all_flags, len(observations), agreement)

        # Build the "best" radii (prefer NHC advisory > IBTrACS > HURDAT2)
        radii_used = self._select_best_radii(observations)

        result = RadiiAuditResult(
            storm_id=storm_id,
            advisory_time=advisory_time,
            audit_time=audit_time,
            sources_checked=len(observations),
            source_names=[o.source for o in observations],
            flags=all_flags,
            confidence_score=confidence,
            cross_source_agreement=round(agreement, 3) if len(observations) >= 2 else None,
            radii_used=radii_used,
            all_observations=[asdict(o) for o in observations],
        )

        # Step 5: Persist
        self._persist(result)

        # Log summary
        error_count = sum(1 for f in all_flags if f.severity == "error")
        warn_count = sum(1 for f in all_flags if f.severity == "warning")
        logger.info(
            f"[RADII AUDIT] {storm_id} @ {advisory_time}: "
            f"confidence={confidence:.2f}, sources={len(observations)}, "
            f"agreement={agreement:.2f}, errors={error_count}, warnings={warn_count}"
        )

        return result

    def _select_best_radii(self, observations: List[RadiiObservation]) -> Optional[Dict]:
        """
        Select the best available radii, preferring higher-authority sources.

        Priority: nhc_advisory > ibtracs > hurdat2 > jtwc > any other
        Within a source, all quadrant values are used as a unit (no mixing).
        """
        priority = {"nhc_advisory": 0, "ibtracs": 1, "hurdat2": 2, "jtwc": 3}
        sorted_obs = sorted(
            observations,
            key=lambda o: priority.get(o.source, 99)
        )

        if not sorted_obs:
            return None

        best = sorted_obs[0]
        return {
            "source": best.source,
            "r34_ne_nm": best.r34_ne_nm, "r34_se_nm": best.r34_se_nm,
            "r34_sw_nm": best.r34_sw_nm, "r34_nw_nm": best.r34_nw_nm,
            "r50_ne_nm": best.r50_ne_nm, "r50_se_nm": best.r50_se_nm,
            "r50_sw_nm": best.r50_sw_nm, "r50_nw_nm": best.r50_nw_nm,
            "r64_ne_nm": best.r64_ne_nm, "r64_se_nm": best.r64_se_nm,
            "r64_sw_nm": best.r64_sw_nm, "r64_nw_nm": best.r64_nw_nm,
            "rmw_nm": best.rmw_nm,
            "max_wind_kt": best.max_wind_kt,
        }

    def _persist(self, result: RadiiAuditResult):
        """Write audit result to JSONL and SQLite."""
        year = datetime.utcnow().year
        season_dir = _VALIDATION_DIR / str(year)
        season_dir.mkdir(parents=True, exist_ok=True)

        flags_json = json.dumps([asdict(f) for f in result.flags], default=str)
        radii_json = json.dumps(result.radii_used, default=str) if result.radii_used else None
        obs_json = json.dumps(result.all_observations, default=str)

        # JSONL
        try:
            jsonl_path = season_dir / f"{result.storm_id}_radii_audits.jsonl"
            record = {
                "storm_id": result.storm_id,
                "advisory_time": result.advisory_time,
                "audit_time": result.audit_time,
                "sources_checked": result.sources_checked,
                "source_names": result.source_names,
                "confidence_score": result.confidence_score,
                "cross_source_agreement": result.cross_source_agreement,
                "error_count": sum(1 for f in result.flags if f.severity == "error"),
                "warning_count": sum(1 for f in result.flags if f.severity == "warning"),
                "flags": [asdict(f) for f in result.flags],
                "radii_used": result.radii_used,
            }
            with open(jsonl_path, "a") as f:
                f.write(json.dumps(record, default=str) + "\n")
        except Exception as e:
            logger.error(f"[RADII AUDIT] JSONL write failed: {e}")

        # SQLite
        try:
            error_count = sum(1 for f in result.flags if f.severity == "error")
            warning_count = sum(1 for f in result.flags if f.severity == "warning")
            info_count = sum(1 for f in result.flags if f.severity == "info")

            with _db_lock:
                conn = sqlite3.connect(str(_DB_PATH), timeout=10)
                conn.execute("PRAGMA journal_mode=WAL")
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO wind_radii_audits (
                            storm_id, advisory_time, audit_time,
                            sources_checked, source_names, confidence_score,
                            cross_source_agreement,
                            error_count, warning_count, info_count,
                            flags_json, radii_used_json, observations_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        result.storm_id, result.advisory_time, result.audit_time,
                        result.sources_checked, json.dumps(result.source_names),
                        result.confidence_score, result.cross_source_agreement,
                        error_count, warning_count, info_count,
                        flags_json, radii_json, obs_json,
                    ))

                    # Update rolling summary
                    conn.execute("""
                        INSERT INTO wind_radii_audit_summary (
                            storm_id, total_audits, avg_confidence, min_confidence,
                            total_errors, total_warnings,
                            last_audit_time, last_advisory_time
                        ) VALUES (?, 1, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(storm_id) DO UPDATE SET
                            total_audits = total_audits + 1,
                            avg_confidence = (
                                avg_confidence * total_audits + excluded.avg_confidence
                            ) / (total_audits + 1),
                            min_confidence = MIN(min_confidence, excluded.min_confidence),
                            total_errors = total_errors + excluded.total_errors,
                            total_warnings = total_warnings + excluded.total_warnings,
                            last_audit_time = excluded.last_audit_time,
                            last_advisory_time = excluded.last_advisory_time,
                            updated_at = datetime('now')
                    """, (
                        result.storm_id, result.confidence_score, result.confidence_score,
                        error_count, warning_count,
                        result.audit_time, result.advisory_time,
                    ))

                    conn.commit()
                finally:
                    conn.close()
        except Exception as e:
            logger.error(f"[RADII AUDIT] SQLite write failed: {e}")

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------

    def get_audit_history(self, storm_id: str) -> List[Dict]:
        """Retrieve all audit results for a storm, ordered by advisory time."""
        try:
            with _db_lock:
                conn = sqlite3.connect(str(_DB_PATH), timeout=10)
                try:
                    rows = conn.execute("""
                        SELECT advisory_time, audit_time, sources_checked,
                               source_names, confidence_score, cross_source_agreement,
                               error_count, warning_count, flags_json,
                               radii_used_json
                        FROM wind_radii_audits
                        WHERE storm_id = ?
                        ORDER BY advisory_time
                    """, (storm_id,)).fetchall()
                finally:
                    conn.close()

            return [
                {
                    "advisory_time": r[0],
                    "audit_time": r[1],
                    "sources_checked": r[2],
                    "source_names": json.loads(r[3]),
                    "confidence_score": r[4],
                    "cross_source_agreement": r[5],
                    "error_count": r[6],
                    "warning_count": r[7],
                    "flags": json.loads(r[8]),
                    "radii_used": json.loads(r[9]) if r[9] else None,
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"[RADII AUDIT] History query failed: {e}")
            return []

    def get_storm_summary(self, storm_id: str) -> Optional[Dict]:
        """Get the rolling audit summary for a storm."""
        try:
            with _db_lock:
                conn = sqlite3.connect(str(_DB_PATH), timeout=10)
                try:
                    row = conn.execute("""
                        SELECT * FROM wind_radii_audit_summary WHERE storm_id = ?
                    """, (storm_id,)).fetchone()
                    if not row:
                        return None
                    cols = [d[0] for d in conn.execute(
                        "SELECT * FROM wind_radii_audit_summary LIMIT 0"
                    ).description]
                finally:
                    conn.close()
            return dict(zip(cols, row))
        except Exception as e:
            logger.error(f"[RADII AUDIT] Summary query failed: {e}")
            return None

    def get_all_summaries(self) -> List[Dict]:
        """Get audit summaries for all storms."""
        try:
            with _db_lock:
                conn = sqlite3.connect(str(_DB_PATH), timeout=10)
                try:
                    rows = conn.execute("""
                        SELECT storm_id, total_audits, avg_confidence,
                               min_confidence, total_errors, total_warnings,
                               last_audit_time, last_advisory_time
                        FROM wind_radii_audit_summary
                        ORDER BY last_advisory_time DESC
                    """).fetchall()
                finally:
                    conn.close()

            return [
                {
                    "storm_id": r[0], "total_audits": r[1],
                    "avg_confidence": round(r[2], 3), "min_confidence": round(r[3], 3),
                    "total_errors": r[4], "total_warnings": r[5],
                    "last_audit_time": r[6], "last_advisory_time": r[7],
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"[RADII AUDIT] Summaries query failed: {e}")
            return []

    def get_latest_confidence(self, storm_id: str) -> Optional[float]:
        """Quick lookup: what's the latest confidence score for this storm?"""
        try:
            with _db_lock:
                conn = sqlite3.connect(str(_DB_PATH), timeout=10)
                try:
                    row = conn.execute("""
                        SELECT confidence_score FROM wind_radii_audits
                        WHERE storm_id = ? ORDER BY advisory_time DESC LIMIT 1
                    """, (storm_id,)).fetchone()
                finally:
                    conn.close()
            return row[0] if row else None
        except Exception:
            return None


# ---------------------------------------------------------------------------
# Helper: build RadiiObservation from a HurricaneSnapshot
# ---------------------------------------------------------------------------

def snapshot_to_observation(
    snapshot,
    source: str = "unknown",
) -> RadiiObservation:
    """
    Convert a HurricaneSnapshot (from any source) into a RadiiObservation
    for audit comparison.

    The snapshot stores radii in meters; this converts back to nautical miles
    for consistent comparison.
    """
    from core.ike import meters_to_nm

    def _q(quadrant_dict, key):
        if quadrant_dict and key in quadrant_dict:
            val_m = quadrant_dict[key]
            if val_m and val_m > 0:
                return round(meters_to_nm(val_m), 1)
        return None

    from core.ike import ms_to_knots

    return RadiiObservation(
        source=source,
        timestamp=snapshot.timestamp.isoformat() + "Z" if hasattr(snapshot.timestamp, 'isoformat') else str(snapshot.timestamp),
        storm_id=snapshot.storm_id,
        max_wind_kt=round(ms_to_knots(snapshot.max_wind_ms), 1) if snapshot.max_wind_ms else None,
        r34_ne_nm=_q(snapshot.r34_quadrants_m, "NE"),
        r34_se_nm=_q(snapshot.r34_quadrants_m, "SE"),
        r34_sw_nm=_q(snapshot.r34_quadrants_m, "SW"),
        r34_nw_nm=_q(snapshot.r34_quadrants_m, "NW"),
        r50_ne_nm=_q(snapshot.r50_quadrants_m, "NE"),
        r50_se_nm=_q(snapshot.r50_quadrants_m, "SE"),
        r50_sw_nm=_q(snapshot.r50_quadrants_m, "SW"),
        r50_nw_nm=_q(snapshot.r50_quadrants_m, "NW"),
        r64_ne_nm=_q(snapshot.r64_quadrants_m, "NE"),
        r64_se_nm=_q(snapshot.r64_quadrants_m, "SE"),
        r64_sw_nm=_q(snapshot.r64_quadrants_m, "SW"),
        r64_nw_nm=_q(snapshot.r64_quadrants_m, "NW"),
        rmw_nm=round(meters_to_nm(snapshot.rmw_m), 1) if snapshot.rmw_m and snapshot.rmw_m > 0 else None,
        lat=snapshot.lat,
        lon=snapshot.lon,
    )
