"""
Interactive Application Confirmation Utility for job-bot
Prompts the user with full job details (Title, Company, Location, Direct Link, Skills)
before applying to any vacancy on any platform.
Enforces minimum AI match score threshold (score >= 70).
"""
import os
import sys

_auto_confirm_all = False


def get_score_threshold() -> int:
    try:
        import yaml
        cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.yaml")
        if os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
                return int(cfg.get("ai", {}).get("score_threshold", 70))
    except Exception:
        pass
    return 70


def ask_confirmation(
    title: str,
    company: str,
    platform: str,
    location: str = "",
    url: str = "",
    skills: list = None,
    ai_score: int = None,
    ai_reason: str = "",
) -> bool:
    """
    Prompt user before applying to a job with complete vacancy details.
    Enforces score >= threshold (default 70).
    Returns:
        True  -> Proceed to apply
        False -> Skip this job
    """
    global _auto_confirm_all
    threshold = get_score_threshold()

    # Strict score enforcement: do not apply if AI score is below threshold
    if ai_score is not None and ai_score < threshold:
        print(f"\n  ⏭️ SKIPPED: AI Score {ai_score}% is below threshold {threshold}% (Job: {title} @ {company or 'Direct Employer'})")
        return False

    # Location policy: Apply only for Bangalore/Bengaluru for on-site; if remote, allow Pune, Mumbai, Bangalore, or general Remote.
    if location:
        loc_lower = str(location).lower()
        is_remote = any(r in loc_lower for r in ["remote", "work from home", "wfh", "anywhere", "telecommute"])
        is_blr = any(b in loc_lower for b in ["bangalore", "bengaluru", "blr"])
        if not (is_remote or is_blr):
            reason = "On-site Pune/Mumbai rejected (only allowed if Remote)" if ("pune" in loc_lower or "mumbai" in loc_lower) else f"Location '{location}' is not Bangalore (Remote only allowed for Pune/Mumbai)"
            print(f"\n  ⏭️ SKIPPED (Location Policy): {reason} (Job: {title} @ {company})")
            return False

    if _auto_confirm_all:
        return True

    # If running non-interactively (e.g. CI/CD / pipe), proceed if score meets threshold
    if not sys.stdin.isatty():
        return True

    print("\n" + "═" * 68)
    print(f"🎯 PLATFORM : [{platform.upper()}]")
    print(f"📌 TITLE    : {title}")
    if company:
        print(f"🏢 COMPANY  : {company}")
    if location:
        print(f"📍 LOCATION : {location}")
    if url:
        print(f"🔗 LINK     : {url}")
    if skills:
        skills_str = ", ".join(str(s).strip() for s in skills if str(s).strip())
        if skills_str:
            print(f"🛠️  SKILLS   : {skills_str}")
    if ai_score is not None:
        score_icon = "🟢" if ai_score >= 80 else ("🟡" if ai_score >= 70 else "🔴")
        print(f"🤖 AI MATCH : {score_icon} {ai_score}/100" + (f" ({ai_reason})" if ai_reason else ""))
    print("═" * 68)

    while True:
        try:
            choice = input("👉 Apply to this job? [Y/n/a/q] (Y: Yes, n: Skip, a: All, q: Quit): ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n⏹️ Stopped by user.")
            sys.exit(0)

        if choice in ("", "y", "yes"):
            return True
        elif choice in ("n", "no"):
            print(f"⏩ Skipped by user: {title}")
            return False
        elif choice in ("a", "all"):
            _auto_confirm_all = True
            print("⚡ Auto-confirm enabled for all remaining jobs in this run.")
            return True
        elif choice in ("q", "quit"):
            print("\n🛑 Application run stopped at your request.")
            sys.exit(0)
        else:
            print("Please enter 'y' to apply, 'n' to skip, 'a' for all, or 'q' to quit.")
