"""
Skill Matcher Utility for job-bot
Filters out irrelevant technologies (Java, .NET, PHP, C++, etc.)
and non-software roles (Cinematographer, Chemical Engineer, Sales, Marketing, etc.)
so ONLY vacancies matching your real candidate stack are processed.
"""
import os
import re
import urllib.parse
import yaml

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config.yaml")

DEFAULT_USER_SKILLS = [
    "react", "react.js", "node", "node.js", "nodejs",
    "javascript", "typescript", "python", "express", "express.js",
    "mongodb", "next.js", "nextjs", "full stack", "fullstack",
    "frontend", "front-end", "backend", "web developer",
    "software engineer", "software development engineer", "sde",
    "rest api", "html", "css", "tailwind"
]

DEFAULT_VETO_SKILLS = [
    # Non-matching tech stacks
    "java", "spring boot", "springboot", ".net", "dotnet", "dot net",
    "c#", "asp.net", "php", "laravel", "c++", "ruby", "rails",
    "angular", "angularjs", "android", "ios", "flutter", "swift",
    "kotlin",
    # Non-software / irrevelant roles injected by portal recommendation carousels
    "cinematographer", "cinematography", "chemical", "chemical engineer",
    "finance", "operations", "marketing", "sales", "civil", "mechanical",
    "electrical", "content writer", "content writing", "graphic design",
    "graphic designer", "video editor", "refrigeration", "electronics",
    "management trainee", "hr", "recruiter", "accounting", "accountant",
    "business development", "bda", "counselor", "data entry", "telecaller",
    "tele-caller", "bpo", "voice process"
]


def load_filters():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
            user_skills = [str(s).lower() for s in cfg.get("user_skills", [])] or DEFAULT_USER_SKILLS
            veto_skills = [str(v).lower() for v in cfg.get("veto_skills", [])] or DEFAULT_VETO_SKILLS
            return user_skills, veto_skills
    except Exception:
        return DEFAULT_USER_SKILLS, DEFAULT_VETO_SKILLS


def slug_to_title(url: str) -> str:
    """Extract human-readable job title from URL slug."""
    try:
        path = urllib.parse.urlparse(url).path
        parts = path.strip("/").split("/")
        if len(parts) >= 3 and parts[0] in ["job", "internship"] and parts[1] == "detail":
            slug = parts[2]
            slug_clean = re.sub(r"\d+$", "", slug)
            slug_clean = re.sub(r"-at-.*$", "", slug_clean)
            slug_clean = re.sub(r"-(job|internship)-.*$", "", slug_clean)
            slug_clean = re.sub(r"-(job|internship)$", "", slug_clean)
            slug_clean = slug_clean.replace("-", " ").strip().title()
            return slug_clean
    except Exception:
        pass
    return ""


def is_location_allowed(location: str) -> tuple[bool, str]:
    """
    Candidate policy:
    - Apply only for Bangalore / Bengaluru for on-site / hybrid.
    - If remote job, allow Pune, Mumbai, Bangalore, or general Remote / WFH.
    """
    if not location:
        return True, "Location unspecified (Remote eligible)"
    loc = str(location).lower().strip()
    is_remote = any(r in loc for r in ["remote", "work from home", "wfh", "anywhere", "telecommute"])
    is_blr = any(b in loc for b in ["bangalore", "bengaluru", "blr"])
    if is_remote:
        return True, "Remote / WFH permitted (Pune, Mumbai, BLR eligible)"
    if is_blr:
        return True, "Bangalore / Bengaluru on-site permitted"
    if "pune" in loc or "mumbai" in loc:
        return False, "On-site Pune/Mumbai rejected (only allowed if Remote)"
    return False, f"Non-Bangalore on-site rejected ({location.strip()})"


def is_matching_job(title: str, skills: list = None, url: str = "") -> tuple[bool, str]:
    """
    Evaluates whether a vacancy matches candidate tech stack:
    Returns (is_match: bool, reason: str)
    """
    user_skills, veto_skills = load_filters()
    title_lower = (title or "").lower()
    skills_lower = [str(s).lower() for s in (skills or [])]
    url_slug_words = (url or "").lower().replace("-", " ").replace("/", " ").replace("_", " ")

    combined_text = f"{title_lower} {' '.join(skills_lower)} {url_slug_words}"

    # 1. Check for VETO / unwanted technologies or non-software roles
    for veto in veto_skills:
        if veto == "java":
            # Match 'java' as standalone word, never match inside 'javascript'
            if re.search(r"\bjava\b", combined_text, re.IGNORECASE) and not re.search(r"\bjavascript\b", combined_text, re.IGNORECASE):
                return False, f"Contains excluded technology: 'Java'"
        elif veto in [".net", "dot net", "dotnet"]:
            if re.search(r"(\.net|\bdotnet\b|\bdot\s+net\b)", combined_text, re.IGNORECASE):
                return False, f"Contains excluded technology: '.NET'"
        elif veto in ["c#", "c++"]:
            if veto in combined_text:
                return False, f"Contains excluded technology: '{veto}'"
        else:
            pattern = r"\b" + re.escape(veto) + r"\b"
            if re.search(pattern, combined_text, re.IGNORECASE):
                return False, f"Excluded role/technology: '{veto}'"

    # 2. Check for positive skill / stack match in Title or Skills or URL
    # (Must have at least one genuine match with candidate profile)
    matched = []
    for skill in user_skills:
        pattern = r"\b" + re.escape(skill) + r"\b"
        if re.search(pattern, combined_text, re.IGNORECASE):
            matched.append(skill)

    if not matched:
        return False, "Not a software developer / stack role (no match with React/Node/Python/Fullstack)"

    unique_matches = list(dict.fromkeys(matched))
    return True, f"Matches: {', '.join(unique_matches[:4])}"


def evaluate_job_with_ai(title: str, company: str = "", skills: list = None, url: str = "") -> dict:
    """
    Complete hybrid evaluation:
    1. Fast heuristic & veto check.
    2. Deep Gemini AI match scoring (0-100%).
    Returns:
        {
            "matches": bool,
            "score": int,
            "reason": str,
            "matched_skills": list
        }
    """
    is_match, veto_reason = is_matching_job(title, skills, url=url)
    if not is_match:
        return {
            "matches": False,
            "score": 0,
            "reason": veto_reason,
            "matched_skills": []
        }

    try:
        from utils.ai_engine import score_job
        ai_res = score_job(title, company, skills=skills, url=url)
        return {
            "matches": ai_res.get("should_apply", True),
            "score": ai_res.get("score", 75),
            "reason": ai_res.get("reason", veto_reason),
            "matched_skills": ai_res.get("matched_skills", [])
        }
    except Exception:
        return {
            "matches": True,
            "score": 75,
            "reason": veto_reason,
            "matched_skills": []
        }

