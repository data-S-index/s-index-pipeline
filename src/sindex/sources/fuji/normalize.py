from __future__ import annotations

from typing import Any

from sindex.core.dates import _norm_date_iso


def fuji_report_from_response(results: dict[str, Any]) -> dict[str, Any]:
    summary = results.get("summary") or {}
    score_percent = summary.get("score_percent") or {}

    return {
        "fair_score": (score_percent.get("FAIR")),
        "evaluation_date": _norm_date_iso(results.get("end_timestamp")),
        "fuji_metric_version": results.get("metric_version"),
        "fuji_software_version": results.get("software_version"),
    }
