# apply_agent.py — NopeRi automated Naukri job application agent
#
# Usage:
#   python apply_agent.py                     normal run
#   python apply_agent.py --setup             re-run setup wizard
#   python apply_agent.py --dashboard         open HTML dashboard
#   python apply_agent.py --list-applied      print applied job history
#   python apply_agent.py --remove-job HASH   allow re-apply for one job
#   python apply_agent.py --veto list
#   python apply_agent.py --veto add-company "Infosys"
#   python apply_agent.py --veto remove-company "Infosys"
#   python apply_agent.py --veto add-title "walk-in"
#   python apply_agent.py --veto remove-title "walk-in"

import argparse
import csv
import os
import sys
import time
import json
from datetime import datetime, timezone

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from colorama import Fore, Style, init
from dotenv import load_dotenv

from src.client.job_classifier import JobFilterPipeline2
from src.client.naukri_client import NaukriLoginClient
from src.client.job_client import NaukriJobClient
from src.utils.config_manager import ConfigManager, CONFIG_PATH
from src.utils.dbhandler import AppliedJobsDB

load_dotenv()
init(autoreset=True)

# ──────────────────────────────────────────────────────────────────────────────
# Display helpers
# ──────────────────────────────────────────────────────────────────────────────

_LINE = Fore.WHITE + "─" * 68 + Style.RESET_ALL
_THIN = Fore.WHITE + "·" * 68 + Style.RESET_ALL


def _section(title: str) -> None:
    print(f"\n{_LINE}")
    print(f"  {Fore.CYAN}{Style.BRIGHT}{title.upper()}{Style.RESET_ALL}")
    print(_LINE)


def _score_bar(score: int, width: int = 10) -> str:
    filled = int((score / 100) * width)
    color  = Fore.GREEN if score >= 70 else (Fore.YELLOW if score >= 50 else Fore.RED)
    return f"{color}{'█' * filled}{'░' * (width - filled)}{Style.RESET_ALL}"


def _print_job(index: int, total: int, job, score=None, reason=None) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d  %H:%M UTC")
    print(f"\n{_LINE}")
    print(f"  {Fore.CYAN}{Style.BRIGHT}JOB {index}/{total}{Style.RESET_ALL}  {Fore.WHITE}{now}{Style.RESET_ALL}")
    print(_THIN)
    print(f"  {Fore.WHITE}Title   :{Style.RESET_ALL}  {Style.BRIGHT}{job.title}{Style.RESET_ALL}")
    print(f"  {Fore.WHITE}Company :{Style.RESET_ALL}  {Fore.YELLOW}{job.company}{Style.RESET_ALL}")
    print(f"  {Fore.WHITE}Job ID  :{Style.RESET_ALL}  {Fore.BLUE}{job.job_id}{Style.RESET_ALL}")
    print(f"  {Fore.WHITE}URL     :{Style.RESET_ALL}  {Fore.BLUE}https://www.naukri.com/job-listings-{job.job_id}{Style.RESET_ALL}")
    if score is not None:
        sc = Fore.GREEN if score >= 70 else (Fore.YELLOW if score >= 50 else Fore.RED)
        detail = f"  {Fore.WHITE}({reason}){Style.RESET_ALL}" if reason else ""
        print(f"  {Fore.WHITE}Score   :{Style.RESET_ALL}  {sc}{score}/100{Style.RESET_ALL}  {_score_bar(score)}{detail}")
    if job.tags:
        tags = "  ".join(f"{Fore.CYAN}[{t}]{Style.RESET_ALL}" for t in job.tags)
        print(f"  {Fore.WHITE}Tags    :{Style.RESET_ALL}  {tags}")


def _print_pipeline_table(jobs: list) -> None:
    _section(f"AI filter — {len(jobs)} jobs passed")
    W = [4, 35, 28, 6]
    print(f"  {Fore.WHITE}{'#':<{W[0]}}  {'Title':<{W[1]}}  {'Company':<{W[2]}}  {'Score':>{W[3]}}{Style.RESET_ALL}")
    print(f"  {Fore.WHITE}{'─' * sum(W)}{Style.RESET_ALL}")
    for i, job in enumerate(jobs, 1):
        s = job.get("ai_score")
        sc = Fore.GREEN if s and s >= 70 else (Fore.YELLOW if s and s >= 50 else Fore.RED)
        sd = f"{sc}{s:>3}{Style.RESET_ALL}" if s is not None else "  ?"
        print(
            f"  {Fore.CYAN}{i:<{W[0]}}{Style.RESET_ALL}  "
            f"{(job.get('title') or '')[:W[1]]:<{W[1]}}  "
            f"{Fore.YELLOW}{(job.get('company') or '')[:W[2]]:<{W[2]}}{Style.RESET_ALL}  "
            f"{sd}"
        )


def _print_fetch_row(keyword, location, exp, page, fetched, new) -> None:
    nc = Fore.GREEN if new > 0 else Fore.WHITE
    print(
        f"  {Fore.WHITE}[{keyword[:30]:<30} | {(location or 'All India')[:12]:<12} | exp={exp} | p{page}]{Style.RESET_ALL}"
        f"  {Fore.WHITE}{fetched:>3} fetched  {nc}{new:>3} new{Style.RESET_ALL}"
    )


def _print_summary(total, allowed, applied, skip_ext, skip_dup, failed, skip_user=0) -> None:
    _section("run summary")
    rows = [
        ("Jobs fetched (total unique)", str(total),     Fore.WHITE),
        ("Jobs passed filter",          str(allowed),   Fore.CYAN),
        ("Applied successfully",        str(applied),   Fore.GREEN),
        ("Skipped (external apply)",    str(skip_ext),  Fore.YELLOW),
        ("Skipped (already applied)",   str(skip_dup),  Fore.YELLOW),
        ("Skipped (by user)",           str(skip_user), Fore.YELLOW),
        ("Failed",                      str(failed),    Fore.RED),
    ]
    for label, val, color in rows:
        print(f"  {Fore.WHITE}{label:<30}{Style.RESET_ALL}  {color}{Style.BRIGHT}{val}{Style.RESET_ALL}")
    print(_LINE + "\n")


# ──────────────────────────────────────────────────────────────────────────────
# CLI args
# ──────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="apply_agent",
        description="NopeRi — Naukri automated job application agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Veto examples:\n"
            "  --veto list\n"
            "  --veto add-company Infosys\n"
            "  --veto remove-company Infosys\n"
            "  --veto add-title walk-in\n"
            "  --veto remove-title walk-in"
        ),
    )
    p.add_argument("--setup",        action="store_true", help="Re-run the setup wizard")
    p.add_argument("--dashboard",    action="store_true", help="Open HTML dashboard and exit")
    p.add_argument("--list-applied", action="store_true", help="Print applied job history and exit")
    p.add_argument("--remove-job",   metavar="HASH",      help="Remove a job from history (enables re-apply)")
    p.add_argument("--veto",         nargs="+",           metavar="ARG", help="Manage veto list — see below")
    p.add_argument("--auto", "-y",   action="store_true", help="Auto-apply to all eligible jobs without confirmation prompts")
    return p.parse_args()


# ──────────────────────────────────────────────────────────────────────────────
# Utility command handlers
# ──────────────────────────────────────────────────────────────────────────────

def _handle_veto(args_list: list, cfg: ConfigManager) -> None:
    veto   = cfg.load_veto()
    action = args_list[0].lower() if args_list else "list"

    if action == "list":
        print(f"\n{Fore.CYAN}── Company vetoes {'─'*40}{Style.RESET_ALL}")
        for c in sorted(veto.get("companies", [])):
            print(f"  {Fore.YELLOW}{c}{Style.RESET_ALL}")
        print(f"\n{Fore.CYAN}── Title vetoes {'─'*42}{Style.RESET_ALL}")
        for t in sorted(veto.get("titles", [])):
            print(f"  {Fore.WHITE}{t}{Style.RESET_ALL}")
        return

    if len(args_list) < 2:
        print(f"{Fore.RED}  Usage: --veto {action} <value>{Style.RESET_ALL}")
        return

    value = " ".join(args_list[1:]).lower().strip()

    if action == "add-company":
        lst = veto.setdefault("companies", [])
        if value not in [x.lower() for x in lst]:
            lst.append(value); cfg.save_veto(veto)
            print(f"{Fore.GREEN}  ✓ Added company veto: {value}{Style.RESET_ALL}")
        else:
            print(f"{Fore.YELLOW}  Already in list: {value}{Style.RESET_ALL}")

    elif action == "remove-company":
        before = len(veto.get("companies", []))
        veto["companies"] = [x for x in veto.get("companies", []) if x.lower() != value]
        if len(veto["companies"]) < before:
            cfg.save_veto(veto); print(f"{Fore.GREEN}  ✓ Removed: {value}{Style.RESET_ALL}")
        else:
            print(f"{Fore.YELLOW}  Not found: {value}{Style.RESET_ALL}")

    elif action == "add-title":
        lst = veto.setdefault("titles", [])
        if value not in [x.lower() for x in lst]:
            lst.append(value); cfg.save_veto(veto)
            print(f"{Fore.GREEN}  ✓ Added title veto: {value}{Style.RESET_ALL}")
        else:
            print(f"{Fore.YELLOW}  Already in list: {value}{Style.RESET_ALL}")

    elif action == "remove-title":
        before = len(veto.get("titles", []))
        veto["titles"] = [x for x in veto.get("titles", []) if x.lower() != value]
        if len(veto["titles"]) < before:
            cfg.save_veto(veto); print(f"{Fore.GREEN}  ✓ Removed: {value}{Style.RESET_ALL}")
        else:
            print(f"{Fore.YELLOW}  Not found: {value}{Style.RESET_ALL}")

    else:
        print(f"{Fore.RED}  Unknown action: {action}{Style.RESET_ALL}")
        print("  Valid: list | add-company | remove-company | add-title | remove-title")


def _handle_list_applied(db: AppliedJobsDB) -> None:
    rows = db.list_applied(limit=50)
    if not rows:
        print(f"\n{Fore.YELLOW}  No applied jobs on record yet.{Style.RESET_ALL}")
        return

    _section(f"applied jobs (last {len(rows)})")
    print(f"  {'HASH':<18}  {'Score':>5}  {'Title':<34}  {'Company':<24}  Applied At")
    print(f"  {'─'*18}  {'─'*5}  {'─'*34}  {'─'*24}  {'─'*19}")
    for row in rows:
        sc = row["ai_score"]
        sc_str = f"{sc:>5}" if sc is not None else "    ?"
        sc_color = Fore.GREEN if sc and sc >= 70 else (Fore.YELLOW if sc and sc >= 50 else Fore.WHITE)
        print(
            f"  {Fore.CYAN}{row['hash']:<18}{Style.RESET_ALL}  "
            f"{sc_color}{sc_str}{Style.RESET_ALL}  "
            f"{(row['title'] or '')[:34]:<34}  "
            f"{Fore.YELLOW}{(row['company'] or '')[:24]:<24}{Style.RESET_ALL}  "
            f"{(row['applied_at'] or '')[:19]}"
        )
    print(f"\n  Total on record: {db.total()}")
    print("  Use --remove-job <HASH> to allow re-applying to a job.\n")


def _handle_dashboard(db: AppliedJobsDB) -> None:
    import json
    import sqlite3
    import tempfile
    import webbrowser
    from pathlib import Path

    total = db.total()
    if not total:
        print(f"\n{Fore.YELLOW}  No applications on record yet.{Style.RESET_ALL}\n")
        return

    conn = sqlite3.connect(db.db_path)
    cur  = conn.cursor()

    cur.execute("""
        SELECT substr(applied_at,1,10) day, COUNT(*) n
        FROM applied_jobs GROUP BY day ORDER BY day DESC LIMIT 30
    """)
    by_day = list(reversed(cur.fetchall()))

    cur.execute("""
        SELECT company, COUNT(*) n FROM applied_jobs
        WHERE company != '' GROUP BY company ORDER BY n DESC LIMIT 8
    """)
    top_co = cur.fetchall()

    cur.execute("""
        SELECT
            SUM(CASE WHEN ai_score >= 70 THEN 1 ELSE 0 END),
            SUM(CASE WHEN ai_score >= 50 AND ai_score < 70 THEN 1 ELSE 0 END),
            SUM(CASE WHEN ai_score IS NOT NULL AND ai_score < 50 THEN 1 ELSE 0 END),
            SUM(CASE WHEN ai_score IS NULL THEN 1 ELSE 0 END)
        FROM applied_jobs
    """)
    high, mid, low, no_sc = cur.fetchone() or (0, 0, 0, 0)

    cur.execute("""
        SELECT title, company, ai_score, applied_at FROM applied_jobs
        ORDER BY applied_at DESC LIMIT 20
    """)
    recent = [{"title":r[0]or"","company":r[1]or"","score":r[2],"date":(r[3]or"")[:10]}
              for r in cur.fetchall()]
    conn.close()

    days_js   = json.dumps([d for d,_ in by_day])
    counts_js = json.dumps([n for _,n in by_day])
    comp_js   = json.dumps([c for c,_ in top_co])
    comp_n_js = json.dumps([n for _,n in top_co])
    recent_js = json.dumps(recent)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NopeRi Dashboard</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root{{--bg:#0d1117;--surf:#161b22;--surf2:#1c2128;--border:#30363d;--accent:#58a6ff;--ok:#3fb950;--warn:#d29922;--err:#f85149;--fg:#c9d1d9;--fg-m:#8b949e;color-scheme:dark}}
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
html,body{{background:var(--bg);color:var(--fg);font-family:'Inter',sans-serif;font-size:14px;line-height:1.6}}
body{{padding:32px max(16px,3vw) 64px}}
.logo{{font-family:'JetBrains Mono',monospace;font-size:20px;font-weight:500;color:#fff;margin-bottom:28px}}
.logo em{{color:var(--accent);font-style:normal}}
.stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:28px}}
.stat{{background:var(--surf);border:1px solid var(--border);border-radius:8px;padding:16px 20px}}
.stat .val{{font-size:28px;font-weight:600;font-family:'JetBrains Mono',monospace;margin-bottom:2px}}
.stat .lbl{{font-size:12px;color:var(--fg-m)}}
.charts{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:28px}}
@media(max-width:600px){{.charts{{grid-template-columns:1fr}}}}
.chart-box{{background:var(--surf);border:1px solid var(--border);border-radius:8px;padding:20px}}
.chart-box h3{{font-size:13px;color:var(--fg-m);margin-bottom:16px;text-transform:uppercase;letter-spacing:.5px}}
table{{width:100%;border-collapse:collapse;background:var(--surf);border:1px solid var(--border);border-radius:8px;overflow:hidden}}
th{{text-align:left;padding:10px 14px;font-size:12px;color:var(--fg-m);text-transform:uppercase;letter-spacing:.4px;border-bottom:1px solid var(--border)}}
td{{padding:10px 14px;border-bottom:1px solid var(--border);font-size:13px}}
tr:last-child td{{border-bottom:none}}
.pill{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;font-family:'JetBrains Mono',monospace}}
.pill.hi{{background:#0a1f12;color:var(--ok)}}
.pill.mid{{background:#1a1505;color:var(--warn)}}
.pill.lo{{background:#1f0a0a;color:var(--err)}}
.pill.ns{{background:var(--surf2);color:var(--fg-m)}}
</style>
</head>
<body>
<div class="logo">Nope<em>Ri</em> &mdash; Dashboard</div>
<div class="stats">
  <div class="stat"><div class="val" style="color:var(--accent)">{total}</div><div class="lbl">Total Applied</div></div>
  <div class="stat"><div class="val" style="color:var(--ok)">{high or 0}</div><div class="lbl">Score &ge;70</div></div>
  <div class="stat"><div class="val" style="color:var(--warn)">{mid or 0}</div><div class="lbl">Score 50&ndash;69</div></div>
  <div class="stat"><div class="val" style="color:var(--err)">{low or 0}</div><div class="lbl">Score &lt;50</div></div>
  <div class="stat"><div class="val" style="color:var(--fg-m)">{no_sc or 0}</div><div class="lbl">No Score</div></div>
</div>
<div class="charts">
  <div class="chart-box"><h3>Applications per Day (last 30)</h3><canvas id="cDay"></canvas></div>
  <div class="chart-box"><h3>Top Companies</h3><canvas id="cCo"></canvas></div>
</div>
<table>
  <thead><tr><th>Title</th><th>Company</th><th>Score</th><th>Date</th></tr></thead>
  <tbody id="rows"></tbody>
</table>
<script>
const days={days_js},counts={counts_js},comps={comp_js},compN={comp_n_js},recent={recent_js};
const opt={{responsive:true,plugins:{{legend:{{display:false}}}},scales:{{x:{{ticks:{{color:'#8b949e',maxRotation:45}},grid:{{color:'#30363d'}}}},y:{{ticks:{{color:'#8b949e'}},grid:{{color:'#30363d'}}}}}}}};
new Chart(document.getElementById('cDay'),{{type:'bar',data:{{labels:days,datasets:[{{data:counts,backgroundColor:'#1f6feb',borderRadius:4}}]}},options:opt}});
new Chart(document.getElementById('cCo'),{{type:'bar',data:{{labels:comps,datasets:[{{data:compN,backgroundColor:'#388bfd',borderRadius:4}}]}},options:{{...opt,indexAxis:'y'}}}});
const tb=document.getElementById('rows');
recent.forEach(r=>{{
  const sc=r.score;
  const cls=sc==null?'ns':sc>=70?'hi':sc>=50?'mid':'lo';
  const sv=sc==null?'—':sc;
  tb.innerHTML+=`<tr><td>${{r.title}}</td><td style="color:#d29922">${{r.company}}</td><td><span class="pill ${{cls}}">${{sv}}</span></td><td style="color:#8b949e">${{r.date}}</td></tr>`;
}});
</script>
</body>
</html>"""

    path = Path(tempfile.mktemp(suffix=".html"))
    path.write_text(html, encoding="utf-8")
    webbrowser.open(f"file:///{path.as_posix()}")
    print("  Dashboard opened in browser.")


# ──────────────────────────────────────────────────────────────────────────────
# Job fetching
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_jobs(jc: NaukriJobClient, config: dict) -> list:
    queries    = config.get("queries", [])
    search_cfg = config.get("search", {})
    exp        = config.get("profile", {}).get("experience_years", 2)
    job_age    = search_cfg.get("job_age", 2)
    pages      = search_cfg.get("pages_per_query", 1)

    seen, all_jobs = set(), []
    _section(f"fetching jobs  ({len(queries)} queries × {pages} page)")

    for q in queries:
        for page in range(1, pages + 1):
            try:
                jobs     = jc.search_jobs(keyword=q["keyword"], location=q.get("location",""), page=page, job_age=job_age, experience=exp)
                new_jobs = [j for j in jobs if j.job_id not in seen and not seen.add(j.job_id)]
                all_jobs.extend(new_jobs)
                _print_fetch_row(q["keyword"], q.get("location",""), exp, page, len(jobs), len(new_jobs))
                if not jobs:
                    break
                time.sleep(1.2)
            except Exception as e:
                print(f"  {Fore.RED}[FAIL]{Style.RESET_ALL}  {q['keyword']} @ {q.get('location','')}  ->  {e}")
                time.sleep(3)

    print(f"\n  {Fore.CYAN}Total unique jobs: {Style.BRIGHT}{len(all_jobs)}{Style.RESET_ALL}")
    return all_jobs


DISCOVERED_JOBS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "discovered_jobs.json")


def is_location_eligible(location_str: str) -> tuple[bool, str]:
    """
    Candidate Location Policy:
    - On-site / Hybrid: ONLY Bangalore / Bengaluru.
    - Remote / Work From Home: Remote is allowed; Pune and Mumbai are allowed for Remote jobs.
    - On-site Pune or Mumbai: NOT allowed.
    - Non-Bangalore on-site (Hyderabad, Delhi, Chennai, Gurgaon, Noida, etc.): NOT allowed.
    """
    if not location_str:
        return True, "Location unspecified (Remote eligible)"
    loc = str(location_str).lower()
    is_remote = any(r in loc for r in ["remote", "work from home", "wfh", "anywhere", "telecommute"])
    is_blr = any(b in loc for b in ["bangalore", "bengaluru", "blr"])
    if is_remote:
        return True, "Remote / WFH permitted"
    if is_blr:
        return True, "Bangalore / Bengaluru on-site permitted"
    if "pune" in loc or "mumbai" in loc:
        return False, "On-site Pune/Mumbai rejected (only allowed if Remote)"
    return False, f"Non-Bangalore on-site rejected ({location_str.strip()})"


def _save_discovered_jobs(scored_jobs: list, raw_jobs: list, db: AppliedJobsDB) -> None:
    raw_map = {str(getattr(j, "job_id", "") or ""): j for j in raw_jobs if getattr(j, "job_id", None)}
    existing = {}
    if os.path.exists(DISCOVERED_JOBS_FILE):
        try:
            with open(DISCOVERED_JOBS_FILE, "r", encoding="utf-8") as f:
                for item in json.load(f):
                    jid = str(item.get("job_id") or "")
                    if jid:
                        existing[jid] = item
        except Exception:
            pass

    now_iso = datetime.now(timezone.utc).isoformat()
    for s in scored_jobs:
        jid = str(s.get("job_id") or "")
        if not jid:
            continue
        raw_j = raw_map.get(jid)
        is_applied = db.has_applied(jid, s.get("company", ""))
        ai_score = int(s.get("ai_score") or 0)
        loc = s.get("location", "")
        loc_ok, loc_reason = is_location_eligible(loc)

        if is_applied:
            status = "Applied"
        elif ai_score >= 70 and loc_ok:
            status = "Ready to Apply"
        else:
            status = "Needs Review"

        url = getattr(raw_j, "apply_link", "") or f"https://www.naukri.com/job-listings-{jid}"
        salary = getattr(raw_j, "salary", "") or "₹3.5 - 5.0 LPA (Target: ₹4 LPA)"
        exp_min = s.get("experience_min", 0)
        exp_max = s.get("experience_max", 2)
        exp_str = f"{exp_min} - {exp_max} Years" if exp_max > 0 else "0 - 1 Years (Fresher)"

        existing[jid] = {
            "job_id": jid,
            "title": s.get("title", ""),
            "company": s.get("company", ""),
            "location": s.get("location", ""),
            "experience": exp_str,
            "salary": salary,
            "tags": s.get("tags", []),
            "description": s.get("description", ""),
            "url": url,
            "ai_score": ai_score,
            "ai_reason": s.get("ai_reason", ""),
            "applied": is_applied,
            "status": status,
            "updated_at": now_iso
        }

    try:
        with open(DISCOVERED_JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(list(existing.values()), f, indent=2)
    except Exception as e:
        print(f"Error saving discovered jobs: {e}")


def _mark_job_applied_in_discovered(job_id: str) -> None:
    if not os.path.exists(DISCOVERED_JOBS_FILE):
        return
    try:
        with open(DISCOVERED_JOBS_FILE, "r", encoding="utf-8") as f:
            jobs_list = json.load(f)
        for j in jobs_list:
            if str(j.get("job_id")) == str(job_id):
                j["applied"] = True
                j["status"] = "Applied"
                j["applied_at"] = datetime.now(timezone.utc).isoformat()
        with open(DISCOVERED_JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(jobs_list, f, indent=2)
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Core agent
# ──────────────────────────────────────────────────────────────────────────────

def run_agent(cfg: ConfigManager, db: AppliedJobsDB, auto_confirm: bool = False) -> dict:
    """
    Full search -> score -> apply pipeline.
    Returns stats dict: {total, applied, skipped_ext, skipped_dup, failed}.
    Called by both CLI and cron/scheduler.py.
    """
    from cron.telegram_alert import (
        alert_apply, alert_failed, alert_questionnaire,
        alert_run_summary, alert_run_started,
    )

    config = cfg.config
    tg_cfg = config.get("telegram", {})
    tg_on  = tg_cfg.get("enabled", False)
    alerts = tg_cfg.get("alerts", {})

    # ── Login ─────────────────────────────────────────────────────────────────
    _section("logging in to naukri")
    session = config.get("session", {})
    nauk_at = session.get("nauk_at", "")

    # Check if nauk_at token is expired
    is_expired = False
    if nauk_at:
        try:
            import base64
            parts = nauk_at.split(".")
            if len(parts) >= 2:
                payload = json.loads(base64.urlsafe_b64decode(parts[1] + "===").decode("utf-8"))
                exp = payload.get("exp", 0)
                if exp and time.time() > (exp - 120):
                    is_expired = True
        except Exception:
            is_expired = True

    client = None
    if nauk_at and not is_expired:
        try:
            client = NaukriLoginClient.from_browser_cookies(nauk_at, session.get("nauk_rt",""))
            print(f"  {Fore.GREEN}Resumed session from stored cookies{Style.RESET_ALL}")
        except Exception:
            client = None

    if client is None:
        username = os.getenv("NAUKRI_USERNAME", "")
        password = os.getenv("PASSWORD", "")
        if not username or not password:
            print(f"  {Fore.RED}No credentials found.{Style.RESET_ALL}")
            print(f"  Add NAUKRI_USERNAME and PASSWORD to .env, or run --setup.")
            sys.exit(1)
        client = NaukriLoginClient(username, password)
        client.login()
        print(f"  {Fore.GREEN}Logged in as {Fore.YELLOW}{username}{Style.RESET_ALL}")
        # Save fresh token to config.json
        if client.naukri_session and client.naukri_session.bearer_token:
            config.setdefault("session", {})
            config["session"]["nauk_at"] = client.naukri_session.bearer_token
            try:
                with open("config.json", "w", encoding="utf-8") as f:
                    json.dump(config, f, indent=2)
            except Exception:
                pass

    if tg_on and alerts.get("on_scheduler_start", True):
        alert_run_started("manual")

    # ── Fetch ─────────────────────────────────────────────────────────────────
    jc   = NaukriJobClient(client, config=config)
    jobs = _fetch_jobs(jc, config)

    if not jobs:
        print(f"\n{Fore.YELLOW}  No jobs found.{Style.RESET_ALL}")
        return {"total":0,"applied":0,"skipped_ext":0,"skipped_dup":0,"failed":0}

    # ── AI filter ─────────────────────────────────────────────────────────────
    if cfg.get_ai_key():
        _section("running AI filter pipeline")
        pipeline   = JobFilterPipeline2(config_manager=cfg)
        final_jobs = pipeline.run(jobs)
        score_map  = {j["job_id"]: j for j in final_jobs}
        allow      = set(score_map)
        _print_pipeline_table(final_jobs)
        scored_pool = getattr(pipeline, "all_scored_jobs", final_jobs)
        _save_discovered_jobs(scored_pool, jobs, db)
    else:
        _section("running rules & veto filter (no ai key set)")
        pipeline = JobFilterPipeline2(config_manager=cfg)
        norm = pipeline.normalize_jobs(jobs)
        filtered = pipeline.dedup(norm)
        filtered = pipeline.hard_veto(filtered)
        filtered = pipeline.experience_filter(filtered)
        filtered = pipeline.desc_red_flag_check(filtered)
        filtered = pipeline.title_filter(filtered)
        filtered = pipeline.company_veto(filtered)
        score_map = {j["job_id"]: j for j in filtered}
        allow = set(score_map)
        print(f"  {Fore.GREEN}Filtered {len(jobs)} jobs down to {len(filtered)} matching your skills and veto rules.{Style.RESET_ALL}")
        _save_discovered_jobs(filtered, jobs, db)

    # ── Questionnaire AI ──────────────────────────────────────────────────────
    _qa = None
    if not config.get("questionnaire", {}).get("skip_ai", False):
        try:
            from src.client.questionnaire_ai import QuestionnaireAI
            _qa = QuestionnaireAI(cfg)
        except Exception as e:
            print(f"  {Fore.YELLOW}Questionnaire AI unavailable: {e}{Style.RESET_ALL}")

    # ── Apply loop ─────────────────────────────────────────────────────────────
    allowed        = [j for j in jobs if j.job_id in allow]
    applied        = 0
    skip_ext       = 0
    skip_dup       = 0
    skip_user      = 0
    failed         = 0
    auto_apply_all = auto_confirm

    _section(f"reviewing & applying to {len(allowed)} filtered jobs")

    for idx, job in enumerate(allowed, 1):
        meta   = score_map.get(job.job_id, {})
        score  = meta.get("ai_score")
        reason = meta.get("ai_reason")

        _print_job(idx, len(allowed), job, score, reason)

        if db.has_applied(job.job_id, job.company):
            print(f"  {Fore.YELLOW}Status  :  Skipped — already applied previously{Style.RESET_ALL}")
            skip_dup += 1
            continue

        loc_val = getattr(job, "location", "") or meta.get("location", "")
        loc_ok, loc_reason = is_location_eligible(loc_val)
        if not loc_ok:
            print(f"  {Fore.YELLOW}Status  :  Skipped — {loc_reason}{Style.RESET_ALL}")
            skip_user += 1
            continue

        mandatory = job.tags[:2] if job.tags else []
        optional  = job.tags[2:] if len(job.tags) > 2 else []

        try:
            if jc.is_external_apply(job.job_id):
                print(f"  {Fore.YELLOW}Status  :  Skipped — external apply (open URL manually){Style.RESET_ALL}")
                skip_ext += 1
                continue

            # Prompt user for confirmation before applying
            if not auto_apply_all and sys.stdin.isatty():
                try:
                    prompt_msg = (
                        f"\n  {Fore.CYAN}{Style.BRIGHT}Apply to '{job.title}' at '{job.company}'?{Style.RESET_ALL} "
                        f"{Fore.WHITE}[Y/n/a/q] (Y: Yes, n: Skip, a: Apply all, q: Quit): {Style.RESET_ALL}"
                    )
                    choice = input(prompt_msg).strip().lower()
                except (EOFError, KeyboardInterrupt):
                    print(f"\n  {Fore.YELLOW}Run interrupted by user.{Style.RESET_ALL}")
                    break

                if choice in ("q", "quit"):
                    print(f"  {Fore.YELLOW}Stopping application run at user request.{Style.RESET_ALL}")
                    break
                elif choice in ("n", "no"):
                    print(f"  {Fore.YELLOW}Status  :  Skipped by user{Style.RESET_ALL}")
                    skip_user += 1
                    continue
                elif choice in ("a", "all"):
                    auto_apply_all = True
                    print(f"  {Fore.GREEN}Auto-confirm enabled for all remaining jobs in this run.{Style.RESET_ALL}")
                # default (Enter or 'y') proceeds to apply!

            result     = jc.apply_job(job, mandatory_skills=mandatory,
                                       optional_skills=optional, source="search")
            job_result = (result.get("jobs") or [{}])[0]

            if job_result.get("questionnaire"):
                print(f"  {Fore.CYAN}           Questionnaire detected, handling automatically{Style.RESET_ALL}")
                if tg_on and alerts.get("on_questionnaire"):
                    alert_questionnaire(job.title, job.company)
                sid = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "0000000"
                precomputed = None
                if _qa:
                    try:
                        precomputed = _qa.answer(job_result["questionnaire"], job)
                    except Exception:
                        pass
                result = jc.handle_static_questionnaire_and_apply(
                    job, questionnaire=job_result["questionnaire"],
                    sid=sid, mandatory_skills=mandatory,
                    optional_skills=optional, source="search",
                    precomputed_answers=precomputed,
                )

            ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
            print(f"  {Fore.GREEN}Status  :  Applied successfully  {Fore.WHITE}at {ts}{Style.RESET_ALL}")
            db.mark_applied(job, ai_score=score)
            _mark_job_applied_in_discovered(job.job_id)
            applied += 1

            if tg_on and alerts.get("on_apply", True):
                alert_apply(job.title, job.company, score, job.job_id)

        except Exception as e:
            if "auth" in str(e).lower() and os.getenv("NAUKRI_USERNAME") and os.getenv("PASSWORD"):
                try:
                    print(f"  {Fore.YELLOW}Auth token expired. Performing auto-relogin & retrying apply...{Style.RESET_ALL}")
                    fresh_client = NaukriLoginClient(os.getenv("NAUKRI_USERNAME"), os.getenv("PASSWORD"))
                    fresh_client.login()
                    jc._client = fresh_client
                    jc._session = fresh_client.session
                    result = jc.apply_job(job, mandatory_skills=mandatory, optional_skills=optional, source="search")
                    job_result = (result.get("jobs") or [{}])[0]
                    if job_result.get("questionnaire"):
                        print(f"  {Fore.CYAN}           Questionnaire detected, handling automatically{Style.RESET_ALL}")
                        sid = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "0000000"
                        precomputed = _qa.answer(job_result["questionnaire"], job) if _qa else None
                        result = jc.handle_static_questionnaire_and_apply(
                            job, questionnaire=job_result["questionnaire"],
                            sid=sid, mandatory_skills=mandatory,
                            optional_skills=optional, source="search",
                            precomputed_answers=precomputed,
                        )
                    ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
                    print(f"  {Fore.GREEN}Status  :  Applied successfully  {Fore.WHITE}at {ts}{Style.RESET_ALL}")
                    db.mark_applied(job, ai_score=score)
                    _mark_job_applied_in_discovered(job.job_id)
                    applied += 1
                    time.sleep(3)
                    continue
                except Exception as retry_err:
                    e = retry_err
            print(f"  {Fore.RED}Status  :  Failed — {e}{Style.RESET_ALL}")
            failed += 1
            if tg_on and alerts.get("on_failure", True):
                alert_failed(job.title, job.company, str(e))

        time.sleep(3)

    # ── Summary ───────────────────────────────────────────────────────────────
    stats = {
        "total":        len(jobs),
        "applied":      applied,
        "skipped_ext":  skip_ext,
        "skipped_dup":  skip_dup,
        "skipped_user": skip_user,
        "failed":       failed,
    }
    _print_summary(stats["total"], len(allowed),
                   applied, skip_ext, skip_dup, failed, skip_user=skip_user)

    if tg_on and alerts.get("on_run_summary", True):
        alert_run_summary(stats["total"], applied, skip_ext, skip_dup, failed)

    return stats


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = _parse_args()
    cfg  = ConfigManager()
    db   = AppliedJobsDB()

    if args.dashboard:
        _handle_dashboard(db)
        sys.exit(0)

    if args.veto:
        _handle_veto(args.veto, cfg)
        sys.exit(0)

    if args.list_applied:
        _handle_list_applied(db)
        sys.exit(0)

    if args.remove_job:
        if db.remove(args.remove_job):
            print(f"{Fore.GREEN}  ✓ Removed {args.remove_job} — job can be re-applied.{Style.RESET_ALL}")
        else:
            print(f"{Fore.YELLOW}  Hash not found: {args.remove_job}{Style.RESET_ALL}")
        sys.exit(0)

    if args.setup or not CONFIG_PATH.exists():
        from src.utils.web_setup import launch as web_launch
        web_launch()
        cfg._config = None  # reload after wizard wrote config.json
        if args.setup:
            sys.exit(0)

    # one-time CSV -> SQLite migration (silent if no CSV)
    if os.path.exists("applied_jobs.csv"):
        count = 0
        try:
            with open("applied_jobs.csv", newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    job_id = (row.get("job_id") or "").strip()
                    if job_id:
                        h = AppliedJobsDB._hash(job_id, "")
                        db._insert_raw(h, job_id, row.get("title",""), row.get("company",""),
                                       "", None, row.get("applied_at", datetime.now(timezone.utc).isoformat()))
                        count += 1
        except Exception:
            pass
        if count:
            print(f"  {Fore.CYAN}Migrated {count} record(s) from CSV -> SQLite{Style.RESET_ALL}")

    run_agent(cfg, db, auto_confirm=getattr(args, "auto", False))
