"""UTC time helpers.

``datetime.utcnow()`` is deprecated as of Python 3.12 (it returns a tz-naive
value that misrepresents itself as local time). ``utcnow()`` here is a drop-in
replacement: it returns a tz-**naive** datetime whose value is identical to the
old call, so all existing arithmetic, ``.isoformat() + "Z"`` serialization, and
comparisons against other naive timestamps keep working byte-for-byte — without
emitting the DeprecationWarning.

Prefer this over reintroducing ``datetime.utcnow()`` anywhere in the codebase.
"""

from __future__ import annotations

from datetime import datetime, timezone


def utcnow() -> datetime:
    """Naive UTC 'now' — drop-in for the deprecated ``datetime.utcnow()``."""
    return datetime.now(timezone.utc).replace(tzinfo=None)
