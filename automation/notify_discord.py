"""
Post the result of an automation workflow run to Discord.

Run by each workflow as its last step, whether or not the job succeeded. It
posts the job's status, the run's report (the lines the script added with
common.report, in the file named by RUN_REPORT) and a link to the run. A
successful run with an empty report (nothing to do) posts nothing.

Uses only the standard library, so it works even if installing the
dependencies failed. DISCORD_WEBHOOK_URL is read from the environment; without
it, nothing is posted.

Usage:
    python automation/notify_discord.py "DataCite harvest" success
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request
from pathlib import Path

# Embed colors per GitHub job status (success, failure, cancelled)
COLORS = {"success": 0x2EA043, "failure": 0xCF222E, "cancelled": 0x8C959F}

# Discord's limit for an embed description
MAX_DESCRIPTION = 4096


def read_report() -> str:
    """Return the run's report, or "" if there is none."""
    path = os.environ.get("RUN_REPORT")
    if not path or not Path(path).is_file():
        return ""
    return Path(path).read_text(encoding="utf-8").strip()


def run_url() -> str | None:
    """Return the URL of the current GitHub Actions run, or None outside Actions."""
    env = os.environ
    if "GITHUB_RUN_ID" not in env:
        return None
    return (
        f"{env['GITHUB_SERVER_URL']}/{env['GITHUB_REPOSITORY']}"
        f"/actions/runs/{env['GITHUB_RUN_ID']}"
    )


def main() -> None:
    """Post the run's status and report to the Discord webhook."""
    if len(sys.argv) != 3:
        sys.exit(f"Usage: {sys.argv[0]} <workflow name> <job status>")
    name, status = sys.argv[1], sys.argv[2]

    webhook = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook:
        print("DISCORD_WEBHOOK_URL is not set; not posting to Discord.")
        return

    report = read_report()
    if status == "success" and not report:
        print("Nothing was done; not posting to Discord.")
        return

    if not report:
        report = "The job stopped before the script reported anything; see the run."
    if len(report) > MAX_DESCRIPTION:
        report = "…\n" + report[-(MAX_DESCRIPTION - 2) :]

    embed = {
        "title": f"{name}: {status}",
        "description": report,
        "color": COLORS.get(status, COLORS["cancelled"]),
    }
    if url := run_url():
        embed["url"] = url

    request = urllib.request.Request(
        webhook,
        data=json.dumps({"embeds": [embed]}).encode(),
        # Discord rejects urllib's default User-Agent
        headers={"Content-Type": "application/json", "User-Agent": "s-index-pipeline"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30):
            pass
    except OSError as e:
        # Don't fail the job over a notification
        print(f"Could not post to Discord: {e}")
        return
    print("Posted to Discord.")


if __name__ == "__main__":
    main()
