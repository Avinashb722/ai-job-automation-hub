"""
AI Engine for job-bot powered by Google Gemini (gemini-3.5-flash)
Provides:
1. Real-time Job Fit Scoring (0-100%) against candidate resume.
2. Screening Question Auto-Answering (Naukri, Internshala, Indeed).
3. Dynamic Tailored Cover Notes.
"""
import os
import re
import json
import warnings
import urllib.parse
from dotenv import load_dotenv

warnings.filterwarnings('ignore')

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
AI_PROVIDER = os.getenv("AI_PROVIDER", "gemini").lower()
OPENAI_COMPAT_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
GROQ_COMPAT_URL = "https://api.groq.com/openai/v1/chat/completions"

# Candidate Profile facts (Avinash Biradar - C:\Users\Hp\Downloads\resume.pdf)
CANDIDATE_PROFILE = {
    "name": "Avinash Biradar",
    "email": "avinashbiradar722@gmail.com",
    "phone": "8746988163",
    "location": "Bengaluru, Karnataka",
    "current_ctc": "0",             # 0 LPA (Fresher)
    "expected_ctc": "400000",        # 4 LPA
    "notice_days": 15,
    "notice_period": "15 Days / Immediate",
    "experience_years": "0",         # Fresher
    "degree": "B.E. in Computer Science and Engineering",
    "college": "Angadi Institute of Technology & Management",
    "cgpa": "8.53",
    "primary_skills": [
        "python", "javascript", "sql", "react", "react.js",
        "django", "node.js", "express.js", "mysql", "supabase", "git", "rest apis"
    ],
    "secondary_skills": ["html5", "css3", "tailwind css", "mongodb", "docker", "unit testing"],
    "projects": [
        "IRIS – AI Voice Desktop Assistant (Python, Automation, 2000+ users)",
        "VTUnotes – Academic study platform (React, Node.js, Express, MongoDB)",
        "LootBazaar – Real-time Deals Aggregator (React, Node.js, Supabase, REST APIs)"
    ],
    "internships": [
        "Software Development Intern at Tech Fortune Technologies (Jan 2026 – May 2026)",
        "Web Development Intern at Tech Octanet Services (Jul 2024 – Aug 2024)"
    ]
}

# Dynamically synchronize profile from config.yaml if present
try:
    import yaml
    _cfg_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yaml")
    if os.path.exists(_cfg_path):
        with open(_cfg_path, "r", encoding="utf-8") as _f:
            _y = yaml.safe_load(_f)
            if _y and "applicant" in _y:
                _app = _y["applicant"]
                if "name" in _app: CANDIDATE_PROFILE["name"] = _app["name"]
                if "email" in _app: CANDIDATE_PROFILE["email"] = _app["email"]
                if "phone" in _app: CANDIDATE_PROFILE["phone"] = _app["phone"]
                if "current_location" in _app: CANDIDATE_PROFILE["location"] = _app["current_location"]
                if "current_ctc" in _app: CANDIDATE_PROFILE["current_ctc"] = str(_app["current_ctc"])
                if "expected_ctc" in _app: CANDIDATE_PROFILE["expected_ctc"] = str(_app["expected_ctc"])
                if "experience_years" in _app: CANDIDATE_PROFILE["experience_years"] = str(_app["experience_years"])
                if "notice_period" in _app:
                    try:
                        CANDIDATE_PROFILE["notice_days"] = int(_app["notice_period"])
                    except ValueError:
                        pass
            if _y and "user_skills" in _y:
                CANDIDATE_PROFILE["primary_skills"] = _y["user_skills"]
except Exception as _e:
    pass

# Cache to avoid duplicate API calls
_score_cache = {}
_qa_cache = {}


def _call_groq_chat(prompt: str, system_instruction: str = "", temperature: float = 0.2) -> str:
    """Invokes Groq Cloud via OpenAI-compatible endpoint."""
    if not GROQ_API_KEY:
        return ""
    try:
        import requests
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json"
        }
        messages = []
        if system_instruction:
            messages.append({"role": "system", "content": system_instruction})
        messages.append({"role": "user", "content": prompt})

        res = requests.post(
            GROQ_COMPAT_URL,
            headers=headers,
            json={
                "model": GROQ_MODEL,
                "messages": messages,
                "temperature": temperature
            },
            timeout=25
        )
        if res.status_code == 200:
            return res.json()["choices"][0]["message"]["content"].strip()
        else:
            print(f"⚠️ Groq HTTP {res.status_code}: {res.text[:120]}")
    except Exception as e:
        print(f"⚠️ Groq call error: {e}")
    return ""


def _call_gemini_chat_internal(prompt: str, system_instruction: str = "", temperature: float = 0.2) -> str:
    """Invokes Google Gemini via Google GenAI or HTTP API."""
    if not GEMINI_API_KEY:
        return ""

    # Method 1: Try google.genai SDK
    try:
        from google import genai
        client = genai.Client(api_key=GEMINI_API_KEY)
        config = {"temperature": temperature}
        if system_instruction:
            config["system_instruction"] = system_instruction
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=config
        )
        if response and response.text:
            return response.text.strip()
    except Exception:
        pass

    # Method 2: Try HTTP via OpenAI-compatible endpoint
    try:
        import requests
        headers = {
            "Authorization": f"Bearer {GEMINI_API_KEY}",
            "Content-Type": "application/json"
        }
        messages = []
        if system_instruction:
            messages.append({"role": "system", "content": system_instruction})
        messages.append({"role": "user", "content": prompt})

        res = requests.post(
            OPENAI_COMPAT_URL,
            headers=headers,
            json={
                "model": GEMINI_MODEL,
                "messages": messages,
                "temperature": temperature
            },
            timeout=25
        )
        if res.status_code == 200:
            data = res.json()
            return data["choices"][0]["message"]["content"].strip()
    except Exception:
        pass

    return ""


def _call_gemini_chat(prompt: str, system_instruction: str = "", temperature: float = 0.2) -> str:
    """Dual AI provider engine: routes to chosen provider (Gemini or Groq) with seamless fallback."""
    provider = (AI_PROVIDER or "gemini").lower()

    if provider == "groq" or (not GEMINI_API_KEY and GROQ_API_KEY):
        res = _call_groq_chat(prompt, system_instruction=system_instruction, temperature=temperature)
        if res:
            return res
        return _call_gemini_chat_internal(prompt, system_instruction=system_instruction, temperature=temperature)

    # Default to Gemini
    res = _call_gemini_chat_internal(prompt, system_instruction=system_instruction, temperature=temperature)
    if res:
        return res

    # Fallback to Groq if Gemini fails or quota is exhausted
    if GROQ_API_KEY:
        print("🔄 Gemini response empty/rate-limited. Seamlessly falling back to Groq...")
        return _call_groq_chat(prompt, system_instruction=system_instruction, temperature=temperature)

    return ""


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


def score_job(title: str, company: str, skills: list = None, url: str = "", description: str = "") -> dict:
    """
    Computes an authentic match score (0-100) between the vacancy and Avinash Biradar's resume.
    Returns:
        {
            "score": int,
            "reason": str,
            "matched_skills": list,
            "should_apply": bool
        }
    """
    cache_key = f"{title.lower()}::{company.lower()}"
    if cache_key in _score_cache:
        return _score_cache[cache_key]

    threshold = get_score_threshold()

    skills = [str(s).strip() for s in (skills or []) if str(s).strip()]
    skills_text = ", ".join(skills) if skills else "General Software Development"

    system_instruction = (
        "You are an expert AI technical recruiter and job-matching engine. "
        "Score candidate fit strictly between 0 and 100 based on the candidate's actual tech stack and resume facts."
    )

    prompt = f"""
CANDIDATE PROFILE:
- Name: {CANDIDATE_PROFILE['name']}
- Primary Stack: {', '.join(CANDIDATE_PROFILE['primary_skills'])}
- Experience: {CANDIDATE_PROFILE['experience_years']} year (Internships in Full Stack & Software Development)
- Education: {CANDIDATE_PROFILE['degree']} (CGPA {CANDIDATE_PROFILE['cgpa']})
- Location: {CANDIDATE_PROFILE['location']}
- Notice Period: {CANDIDATE_PROFILE['notice_period']}

JOB DETAILS:
- Title: {title}
- Company: {company}
- Skills Required: {skills_text}
- Job URL: {url}
- Description Excerpt: {description[:300] if description else 'N/A'}

SCORING GUIDELINES:
- 85-100: Strong Match — Python, React, Node.js, Django, SQL, Full Stack, SDE (0-2 years exp).
- 65-84: Good Match — modern web developer role with substantial overlap.
- 45-64: Average Match — partial stack or junior role with adjacent technologies.
- 0-44: Poor Match / Skip — non-software, sales, or legacy Java/.NET without modern web.

Return ONLY a valid JSON object in this exact format:
{{
  "score": 90,
  "reason": "Strong match with Python, React, and Full Stack stack for 0-2 yrs exp",
  "matched_skills": ["Python", "React", "SQL"]
}}
"""
    raw_ai = _call_gemini_chat(prompt, system_instruction=system_instruction, temperature=0.1)

    result = None
    if raw_ai:
        try:
            cleaned = re.sub(r"```json|```", "", raw_ai).strip()
            match = re.search(r"\{.*\}", cleaned, re.DOTALL)
            if match:
                data = json.loads(match.group(0))
                score = int(data.get("score", 75))
                reason = data.get("reason", "Stack alignment verified")
                matched_skills = data.get("matched_skills", [])
                result = {
                    "score": score,
                    "reason": reason,
                    "matched_skills": matched_skills,
                    "should_apply": score >= threshold
                }
        except Exception:
            pass

    # Deterministic fallback if offline
    if not result:
        title_lower = title.lower()
        skills_lower = [s.lower() for s in skills]
        base_score = 65
        matched = []
        for s in CANDIDATE_PROFILE["primary_skills"]:
            if s in title_lower or any(s in j for j in skills_lower):
                matched.append(s.title())
        if "full stack" in title_lower or "fullstack" in title_lower:
            base_score += 15
        elif "python" in title_lower or "react" in title_lower or "node" in title_lower:
            base_score += 12
        elif "software" in title_lower or "sde" in title_lower:
            base_score += 8
        base_score += min(len(matched) * 3, 12)
        score = min(base_score, 95)
        result = {
            "score": score,
            "reason": f"Matched skills: {', '.join(matched[:3]) if matched else 'Modern Web Stack'}",
            "matched_skills": matched,
            "should_apply": score >= threshold
        }

    _score_cache[cache_key] = result
    return result


def answer_screening_question(
    question_text: str,
    question_type: str = "text",
    options: list = None,
    job_title: str = "",
    company: str = ""
) -> str:
    """
    Answers employer screening questions on Naukri, Internshala, and Indeed.
    Handles objective fields (CTC, notice period, experience, city) deterministically
    and open-ended questions (e.g. 'Why should you be hired?') using Gemini.
    """
    q_norm = (question_text or "").strip().lower()
    options = options or []

    # 1. Deterministic Quick Answers
    if "notice" in q_norm or "joining" in q_norm:
        if options:
            for opt in options:
                opt_l = str(opt).lower()
                if "immediate" in opt_l or "15" in opt_l or "1 month" in opt_l:
                    return str(opt)
            return str(options[0])
        return CANDIDATE_PROFILE["notice_period"]

    if "current ctc" in q_norm or "current salary" in q_norm:
        if options:
            for opt in options:
                opt_l = str(opt).lower()
                if "0" in opt_l or "fresher" in opt_l or "na" in opt_l:
                    return str(opt)
            return str(options[0])
        return "0"

    if "expected ctc" in q_norm or "expected salary" in q_norm:
        if options:
            for opt in options:
                opt_l = str(opt).lower()
                if "4" in opt_l or "3-4" in opt_l or "4-5" in opt_l:
                    return str(opt)
            return str(options[0])
        return "400000"

    if "total experience" in q_norm or "years of experience" in q_norm or "how many years" in q_norm:
        if options:
            for opt in options:
                opt_l = str(opt).lower()
                if "0" in opt_l or "fresher" in opt_l or "0-1" in opt_l:
                    return str(opt)
            return str(options[0])
        return "0"

    if "phone" in q_norm or "mobile" in q_norm or "contact" in q_norm:
        return CANDIDATE_PROFILE["phone"]

    if "email" in q_norm:
        return CANDIDATE_PROFILE["email"]

    if "city" in q_norm or "current location" in q_norm:
        return "Bengaluru"

    if "relocate" in q_norm or "willing to work" in q_norm or "full-time" in q_norm:
        if options:
            for opt in options:
                if str(opt).lower() in ["yes", "willing", "immediately", "full time"]:
                    return str(opt)
            return str(options[0])
        return "Yes"

    if any(neg in q_norm for neg in ["night shift", "rotational shift", "bond", "service agreement"]):
        if options:
            for opt in options:
                if str(opt).lower() in ["no", "not willing"]:
                    return str(opt)
            return str(options[-1])
        return "No"

    # 2. Options selection heuristic
    if options and len(options) > 0:
        for opt in options:
            opt_l = str(opt).lower()
            if opt_l in ["yes", "interested", "immediately", "bachelor's", "bachelor of engineering", "computer science"]:
                return str(opt)
        return str(options[0])

    # 3. AI Generation for Subjective / Assignment Questions (e.g. Internshala "Why should you be hired?")
    qa_key = f"{q_norm}::{job_title.lower()}"
    if qa_key in _qa_cache:
        return _qa_cache[qa_key]

    prompt = f"""
You are {CANDIDATE_PROFILE['name']}, a Full Stack Developer (B.E. in CSE, CGPA 8.53) applying for the role "{job_title or 'Software Engineer'}" at "{company or 'the hiring team'}".
Skills: {', '.join(CANDIDATE_PROFILE['primary_skills'])}.
Projects: {'; '.join(CANDIDATE_PROFILE['projects'])}.
Internships: {'; '.join(CANDIDATE_PROFILE['internships'])}.

Employer Question: "{question_text}"

Provide a concise, professional 2-3 sentence answer directly answering the question, highlighting practical experience, technical skills, and immediate availability. Do not use generic buzzwords or placeholders.
"""
    ai_answer = _call_gemini_chat(prompt, temperature=0.2)
    if ai_answer:
        cleaned_ans = ai_answer.replace('"', '').strip()
        _qa_cache[qa_key] = cleaned_ans
        return cleaned_ans

    # Fallback response
    return (
        f"I have hands-on experience developing web applications and REST APIs using Python, React, and Node.js. "
        f"With internship experience and real-world projects like an AI desktop assistant and deals aggregator, "
        f"I can immediately contribute to the {job_title or 'engineering'} team."
    )


def tailor_cover_note(job_title: str = "", company: str = "", skills: list = None) -> str:
    """Generates an impactful, personalized cover letter for the vacancy."""
    skills_text = ", ".join(skills) if skills else "Python, React, and Node.js"
    prompt = f"""
Write a punchy, professional 3-4 sentence cover note for {CANDIDATE_PROFILE['name']} applying for {job_title or 'Full Stack Developer'} at {company or 'the company'}.
Candidate skills: {skills_text}.
Key achievements: IRIS AI desktop assistant (2000+ users), deals aggregator platform, 1 year internship experience.
Immediate joiner (15 days notice), located in Bengaluru.
Keep it authentic, confident, and direct.
"""
    ai_note = _call_gemini_chat(prompt, temperature=0.3)
    if ai_note and len(ai_note) > 50:
        return ai_note.strip()

    return (
        f"I am a Full Stack Developer with hands-on internship experience building production-ready applications "
        f"using Python, React, Node.js, and SQL. Having developed projects like an AI voice assistant (2,000+ users) "
        f"and real-time deals aggregators, I bring strong problem-solving skills and clean architectural habits. "
        f"I am an immediate joiner (15-day notice) eager to drive impact on your engineering team."
    )
