"""
Data source health monitor — tracks reliability and latency of all weather APIs.

Records success/failure for every API call, computes rolling reliability scores,
and exposes source rankings so the aggregation layer can dynamically prefer
faster and more reliable sources.

Designed for the 2026 hurricane season: when WeatherNext goes live against
real storms, this module will automatically track its accuracy vs traditional
sources and log comparison data for post-season analysis.
"""

import time
import logging
from dataclasses import dataclass, field
from collections import deque
from typing import Optional

logger = logging.getLogger(__name__)

# Rolling window size for reliability calculation
WINDOW_SIZE = 100

# Sources that this monitor tracks
SOURCE_NAMES = [
    "ibtracs",       # NCEI IBTrACS — primary historical track data
    "hurdat2",       # NHC HURDAT2 — Atlantic historical fallback
    "ebtrk",         # CSU EBTRK — extended best track (quadrant radii)
    "nhc_active",    # NHC active storms JSON
    "nhc_forecast",  # NHC GIS forecast track/cone
    "nhc_gfs",       # NOAA NOMADS GFS gridded wind
    "erddap_sst",    # NOAA ERDDAP sea surface temperature
    "google_weather", # Google Maps Platform Weather API
    "nws",           # NWS api.weather.gov
    "open_meteo",    # Open-Meteo (forecast + marine + archive)
    "weathernext",   # Google DeepMind WeatherNext 2 (Vertex AI)
]


@dataclass
class SourceStats:
    """Rolling statistics for a single data source."""
    name: str
    successes: int = 0
    failures: int = 0
    total_calls: int = 0
    total_latency_ms: float = 0.0
    recent_results: deque = field(default_factory=lambda: deque(maxlen=WINDOW_SIZE))
    recent_latencies: deque = field(default_factory=lambda: deque(maxlen=WINDOW_SIZE))
    last_success_time: Optional[float] = None
    last_failure_time: Optional[float] = None
    last_error: Optional[str] = None
    consecutive_failures: int = 0

    @property
    def reliability(self) -> float:
        """Rolling reliability score (0.0 to 1.0) over last WINDOW_SIZE calls."""
        if not self.recent_results:
            return 1.0  # Assume healthy until proven otherwise
        return sum(self.recent_results) / len(self.recent_results)

    @property
    def avg_latency_ms(self) -> float:
        """Average response time over recent calls."""
        if not self.recent_latencies:
            return 0.0
        return sum(self.recent_latencies) / len(self.recent_latencies)

    @property
    def is_healthy(self) -> bool:
        """Source is considered healthy if reliability > 50% and not in extended outage."""
        if self.consecutive_failures >= 5:
            return False
        return self.reliability > 0.5

    @property
    def seconds_since_last_success(self) -> Optional[float]:
        if self.last_success_time is None:
            return None
        return time.time() - self.last_success_time

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "reliability": round(self.reliability, 3),
            "avg_latency_ms": round(self.avg_latency_ms, 1),
            "is_healthy": self.is_healthy,
            "total_calls": self.total_calls,
            "successes": self.successes,
            "failures": self.failures,
            "consecutive_failures": self.consecutive_failures,
            "last_error": self.last_error,
            "seconds_since_last_success": (
                round(self.seconds_since_last_success, 1)
                if self.seconds_since_last_success is not None
                else None
            ),
        }


class SourceHealthMonitor:
    """
    Singleton health monitor for all data sources.

    Usage:
        monitor = SourceHealthMonitor.instance()
        monitor.record_success("ibtracs", latency_ms=142)
        monitor.record_failure("hurdat2", error="NHC timeout after 10s")
        rankings = monitor.get_rankings()
    """

    _instance: Optional["SourceHealthMonitor"] = None

    @classmethod
    def instance(cls) -> "SourceHealthMonitor":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.sources: dict[str, SourceStats] = {
            name: SourceStats(name=name) for name in SOURCE_NAMES
        }
        self._start_time = time.time()

    def record_success(self, source: str, latency_ms: float = 0.0):
        """Record a successful API call."""
        stats = self.sources.get(source)
        if not stats:
            stats = SourceStats(name=source)
            self.sources[source] = stats

        stats.successes += 1
        stats.total_calls += 1
        stats.total_latency_ms += latency_ms
        stats.recent_results.append(1)
        stats.recent_latencies.append(latency_ms)
        stats.last_success_time = time.time()
        stats.consecutive_failures = 0

    def record_failure(self, source: str, error: str = "", latency_ms: float = 0.0):
        """Record a failed API call."""
        stats = self.sources.get(source)
        if not stats:
            stats = SourceStats(name=source)
            self.sources[source] = stats

        stats.failures += 1
        stats.total_calls += 1
        stats.total_latency_ms += latency_ms
        stats.recent_results.append(0)
        stats.recent_latencies.append(latency_ms)
        stats.last_failure_time = time.time()
        stats.last_error = error
        stats.consecutive_failures += 1

        if stats.consecutive_failures >= 3:
            logger.warning(
                f"[SOURCE HEALTH] {source} has {stats.consecutive_failures} "
                f"consecutive failures (reliability: {stats.reliability:.0%})"
            )

    def get_rankings(self) -> list[dict]:
        """
        Get all sources ranked by a composite score:
          score = reliability * 0.7 + speed_factor * 0.3

        Speed factor: normalized so the fastest source gets 1.0.
        """
        active = [s for s in self.sources.values() if s.total_calls > 0]
        if not active:
            return [s.to_dict() for s in self.sources.values()]

        max_latency = max((s.avg_latency_ms for s in active), default=1.0) or 1.0

        ranked = []
        for stats in active:
            speed_factor = 1.0 - (stats.avg_latency_ms / max_latency) if max_latency > 0 else 1.0
            speed_factor = max(0.0, min(1.0, speed_factor))
            composite = stats.reliability * 0.7 + speed_factor * 0.3
            entry = stats.to_dict()
            entry["composite_score"] = round(composite, 3)
            ranked.append(entry)

        ranked.sort(key=lambda x: x["composite_score"], reverse=True)

        # Include sources with zero calls at the bottom
        zero_call = [s.to_dict() for s in self.sources.values() if s.total_calls == 0]
        for entry in zero_call:
            entry["composite_score"] = None
        ranked.extend(zero_call)

        return ranked

    def get_source(self, source: str) -> dict:
        """Get stats for a single source."""
        stats = self.sources.get(source)
        if stats:
            return stats.to_dict()
        return {"name": source, "error": "Unknown source"}

    def get_preferred_source(self, candidates: list[str]) -> str:
        """
        Given a list of candidate sources for the same data type,
        return the healthiest one (highest composite score).
        Falls back to the first candidate if none have been called yet.
        """
        best = candidates[0]
        best_score = -1.0

        for name in candidates:
            stats = self.sources.get(name)
            if stats and stats.total_calls > 0:
                speed_factor = 1.0  # Default if no latency data
                if stats.avg_latency_ms > 0:
                    speed_factor = max(0.0, 1.0 - stats.avg_latency_ms / 5000.0)
                score = stats.reliability * 0.7 + speed_factor * 0.3
                if score > best_score:
                    best_score = score
                    best = name

        return best

    def summary(self) -> dict:
        """Full dashboard summary."""
        uptime_s = time.time() - self._start_time
        return {
            "uptime_seconds": round(uptime_s, 1),
            "sources": self.get_rankings(),
        }
