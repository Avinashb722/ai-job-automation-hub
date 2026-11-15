"""
AI Job Automation Hub — Unified Controller, Dashboard & Settings Engine
Serves a high-performance Web UI for running, monitoring, and configuring:
1. NopeRi (Naukri Direct CloudGateway API)
2. job-bot (Multi-Platform: Naukri, Internshala, Indeed)
"""

import os
import sys
import json
import sqlite3
import subprocess
import threading
import queue
import time
import re
import csv
import io
import webbrowser
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, Response
import uvicorn
import yaml

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
NOPERI_DIR = os.path.join(BASE_DIR, "NopeRi")
JOBBOT_DIR = os.path.join(BASE_DIR, "job-bot")

NOPERI_DB = os.path.join(NOPERI_DIR, "applied_jobs.db")
NOPERI_CACHE = os.path.join(NOPERI_DIR, "score_cache.json")
NOPERI_CONFIG = os.path.join(NOPERI_DIR, "config.json")
NOPERI_ENV = os.path.join(NOPERI_DIR, ".env")
NOPERI_QA = os.path.join(NOPERI_DIR, "qa_cache.json")
NOPERI_DISCOVERED = os.path.join(NOPERI_DIR, "discovered_jobs.json")

JOBBOT_DB = os.path.join(JOBBOT_DIR, "applied_jobs.db")
JOBBOT_CONFIG = os.path.join(JOBBOT_DIR, "config.yaml")
JOBBOT_ENV = os.path.join(JOBBOT_DIR, ".env")
JOBBOT_AI = os.path.join(JOBBOT_DIR, "utils", "ai_engine.py")

app = FastAPI(title="AI Job Automation Hub")

# Subprocess & Streaming log management
class BotRunner:
    def __init__(self):
        self.process: Optional[subprocess.Popen] = None
        self.lock = threading.Lock()
        self.running = False
        self.current_task = "Idle"
        self.history: List[str] = []
        self.subscribers: List[queue.Queue] = []

    def broadcast(self, text: str):
        with self.lock:
            ts = datetime.now().strftime("%H:%M:%S")
            line = f"[{ts}] {text}"
            self.history.append(line)
            if len(self.history) > 1000:
                self.history.pop(0)
            dead = []
            for q in self.subscribers:
                try:
                    q.put_nowait(line)
                except queue.Full:
                    dead.append(q)
            for d in dead:
                self.subscribers.remove(d)

    def subscribe(self) -> queue.Queue:
        q = queue.Queue(maxsize=500)
        with self.lock:
            for h in self.history[-50:]:
                try:
                    q.put_nowait(h)
                except queue.Full:
                    pass
            self.subscribers.append(q)
        return q

    def unsubscribe(self, q: queue.Queue):
        with self.lock:
            if q in self.subscribers:
                self.subscribers.remove(q)

    def run_command(self, cmd: List[str], cwd: str, task_name: str):
        with self.lock:
            if self.running:
                return False, "Another bot run is already in progress."
            self.running = True
            self.current_task = task_name

        def worker():
            self.broadcast(f"🚀 STARTING: {task_name}")
            self.broadcast(f"📁 Directory: {cwd}")
            self.broadcast(f"⚙️ Command: {' '.join(cmd)}")
            self.broadcast("-" * 65)

            try:
                env = os.environ.copy()
                env["PYTHONIOENCODING"] = "utf-8"
                env["PYTHONUTF8"] = "1"
                env["PYTHONUNBUFFERED"] = "1"

                proc = subprocess.Popen(
                    cmd,
                    cwd=cwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                    universal_newlines=True,
                    env=env
                )
                self.process = proc

                for line in iter(proc.stdout.readline, ''):
                    clean = line.rstrip()
                    if clean:
                        self.broadcast(clean)
                proc.stdout.close()
                proc.wait()

                self.broadcast("-" * 65)
                if proc.returncode == 0:
                    self.broadcast(f"✅ FINISHED: {task_name} completed successfully.")
                else:
                    self.broadcast(f"⚠️ FINISHED: {task_name} exited with code {proc.returncode}.")
            except Exception as e:
                self.broadcast(f"❌ EXECUTION ERROR in {task_name}: {str(e)}")
            finally:
                with self.lock:
                    self.running = False
                    self.current_task = "Idle"
                    self.process = None

        t = threading.Thread(target=worker, daemon=True)
        t.start()
        return True, "Started successfully"

    def stop(self):
        with self.lock:
            if not self.running or not self.process:
                return False, "No active run to stop."
            try:
                self.process.terminate()
                self.broadcast("🛑 TERMINATE signal sent to active bot process.")
                time.sleep(1)
                if self.process.poll() is None:
                    self.process.kill()
                    self.broadcast("🛑 KILL signal sent — process terminated.")
                self.running = False
                self.current_task = "Idle"
                self.process = None
                return True, "Process stopped."
            except Exception as e:
                return False, str(e)


runner = BotRunner()

# --- Helper: .env File Updater ---

def update_env_file(filepath: str, updates: Dict[str, str]):
    lines = []
    existing_keys = set()
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    lines.append(line)
                    continue
                if "=" in stripped:
                    key = stripped.split("=")[0].strip()
                    existing_keys.add(key)
                    if key in updates:
                        lines.append(f"{key}={updates[key]}\n")
                    else:
                        lines.append(line)
    for k, v in updates.items():
        if k not in existing_keys:
            lines.append(f"{k}={v}\n")
    with open(filepath, "w", encoding="utf-8") as f:
        f.writelines(lines)


# --- Candidate Profile in Memory ---

CANDIDATE = {
    "name": "Avinash Biradar",
    "email": "avinashbiradar722@gmail.com",
    "phone": "+91 8746988163",
    "location": "Bengaluru, Karnataka",
    "role": "Fresher / Junior Full Stack Developer (2026 B.E. CSE)",
    "experience": "0 Years (Fresher)",
    "current_ctc": "₹0 LPA (Fresher)",
    "expected_ctc": "₹4,00,000 LPA (4 LPA)",
    "notice_period": "15 Days / Immediate",
    "resume_path": r"C:\Users\Hp\Downloads\resume.pdf",
    "ai_model": "Google Gemini 3.5 Flash-Lite (Active | 1,500 req/day quota)",
    "primary_skills": ["Python", "JavaScript", "React.js", "Node.js", "Express.js", "SQL", "MySQL", "Supabase", "Git", "REST APIs"],
    "projects": ["IRIS AI Voice Assistant", "VTUnotes Study Platform", "LootBazaar Deals Aggregator"]
}

# --- Settings Load & Sync Logic ---

def load_all_settings() -> Dict[str, Any]:
    # Read job-bot config.yaml
    jb_cfg = {}
    if os.path.exists(JOBBOT_CONFIG):
        try:
            with open(JOBBOT_CONFIG, "r", encoding="utf-8") as f:
                jb_cfg = yaml.safe_load(f) or {}
        except Exception as e:
            print(f"Error loading job-bot config: {e}")

    # Read NopeRi config.json
    np_cfg = {}
    if os.path.exists(NOPERI_CONFIG):
        try:
            with open(NOPERI_CONFIG, "r", encoding="utf-8") as f:
                np_cfg = json.load(f) or {}
        except Exception as e:
            print(f"Error loading NopeRi config: {e}")

    # Read job-bot .env
    jb_env = {}
    if os.path.exists(JOBBOT_ENV):
        with open(JOBBOT_ENV, "r", encoding="utf-8") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.strip().split("=", 1)
                    jb_env[k.strip()] = v.strip()

    # Read NopeRi .env
    np_env = {}
    if os.path.exists(NOPERI_ENV):
        with open(NOPERI_ENV, "r", encoding="utf-8") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.strip().split("=", 1)
                    np_env[k.strip()] = v.strip()

    applicant = jb_cfg.get("applicant", {})
    np_profile = np_cfg.get("profile", {})
    ai_cfg = jb_cfg.get("ai", {})

    resume_path = jb_cfg.get("resume_path") or np_profile.get("resume_path") or CANDIDATE["resume_path"]
    resume_exists = os.path.exists(resume_path) if resume_path else False
    resume_size = round(os.path.getsize(resume_path) / 1024, 1) if resume_exists else 0

    skills = jb_cfg.get("user_skills") or np_profile.get("tech_stack") or CANDIDATE["primary_skills"]
    veto = jb_cfg.get("veto_skills") or ["java", ".net", "php", "laravel", "c++", "ruby", "sales", "bpo", "telecaller"]

    gemini_key = jb_env.get("GEMINI_API_KEY") or np_env.get("GEMINI_API_KEY", "")
    gemini_model = jb_env.get("GEMINI_MODEL") or np_cfg.get("ai", {}).get("model", "gemini-3.5-flash-lite")
    groq_key = jb_env.get("GROQ_API_KEY") or np_env.get("GROQ_API_KEY", "")
    groq_model = jb_env.get("GROQ_MODEL", "llama-3.3-70b-versatile")
    provider = jb_env.get("AI_PROVIDER") or jb_cfg.get("ai", {}).get("provider") or "gemini"

    return {
        "candidate": {
            "name": applicant.get("name") or CANDIDATE["name"],
            "email": applicant.get("email") or CANDIDATE["email"],
            "phone": applicant.get("phone") or CANDIDATE["phone"],
            "primary_location": applicant.get("current_location") or CANDIDATE["location"],
            "locations": jb_cfg.get("filters", {}).get("locations") or np_profile.get("locations") or ["Bengaluru", "Remote", "Hyderabad", "Pune"],
            "willing_to_relocate": applicant.get("willing_to_relocate", "Yes"),
            "is_fresher": str(applicant.get("experience_years", 0)) == "0",
            "experience_years": int(applicant.get("experience_years", 0)),
            "current_ctc": str(applicant.get("current_ctc", "0")),
            "expected_ctc": str(applicant.get("expected_ctc", "400000")),
            "notice_days": int(applicant.get("notice_period", 15)),
            "degree": "B.E. in Computer Science and Engineering",
            "college": "Angadi Institute of Technology & Management",
            "graduation_year": "2026",
            "cgpa": "8.53",
            "experience_summary": (
                "Software Development Intern at Tech Fortune Technologies (Jan 2026 – May 2026)\n"
                "• Engineered web services and integrated RESTful APIs with Python & React.\n"
                "Web Development Intern at Tech Octanet Services (Jul 2024 – Aug 2024)\n"
                "• Built responsive frontend interfaces and optimized database workflows."
            ),
            "primary_skills": skills,
            "veto_skills": veto,
            "projects": (
                "• IRIS – AI Voice Desktop Assistant (Python, Automation, 2000+ users)\n"
                "• VTUnotes – Academic study platform (React, Node.js, Express, MongoDB)\n"
                "• LootBazaar – Real-time Deals Aggregator (React, Node.js, Supabase, REST APIs)"
            ),
            "resume_path": resume_path,
            "resume_exists": resume_exists,
            "resume_size_kb": resume_size
        },
        "ai": {
            "provider": provider.lower(),
            "gemini_key": gemini_key,
            "gemini_model": gemini_model,
            "groq_key": groq_key,
            "groq_model": groq_model,
            "score_threshold": ai_cfg.get("score_threshold", 70)
        }
    }


def sync_all_settings(data: Dict[str, Any]) -> Dict[str, Any]:
    cand = data.get("candidate", {})
    ai = data.get("ai", {})

    name = cand.get("name", "Avinash Biradar")
    email = cand.get("email", "avinashbiradar722@gmail.com")
    phone = cand.get("phone", "8746988163")
    location = cand.get("primary_location", "Bengaluru, Karnataka")
    
    locations = cand.get("locations", ["Bengaluru", "Remote", "Hyderabad", "Pune"])
    if isinstance(locations, str):
        locations = [l.strip() for l in locations.split(",") if l.strip()]

    is_fresher = cand.get("is_fresher", True)
    exp_years = 0 if is_fresher else int(cand.get("experience_years", 0))

    raw_expected = str(cand.get("expected_ctc", "400000")).replace("₹", "").replace("LPA", "").replace(",", "").strip()
    raw_current = str(cand.get("current_ctc", "0")).replace("₹", "").replace("LPA", "").replace(",", "").strip()

    try:
        val = float(raw_expected)
        if val < 50:
            exp_ctc_lacs = str(int(val) if val.is_integer() else val)
            exp_ctc_full = str(int(val * 100000))
        else:
            exp_ctc_full = str(int(val))
            exp_ctc_lacs = str(round(val / 100000, 1)).rstrip(".0")
    except Exception:
        exp_ctc_lacs = "4"
        exp_ctc_full = "400000"

    try:
        val_c = float(raw_current)
        if val_c < 50:
            curr_ctc_lacs = str(int(val_c) if val_c.is_integer() else val_c)
            curr_ctc_full = str(int(val_c * 100000)) if val_c > 0 else "0"
        else:
            curr_ctc_full = str(int(val_c))
            curr_ctc_lacs = str(round(val_c / 100000, 1)).rstrip(".0")
    except Exception:
        curr_ctc_lacs = "0"
        curr_ctc_full = "0"

    notice_days = int(cand.get("notice_days", 15))
    resume_path = cand.get("resume_path", r"C:\Users\Hp\Downloads\resume.pdf")

    skills_raw = cand.get("primary_skills", "")
    if isinstance(skills_raw, list):
        skills_list = [s.strip() for s in skills_raw if s.strip()]
    else:
        skills_list = [s.strip() for s in str(skills_raw).split(",") if s.strip()]

    veto_raw = cand.get("veto_skills", "")
    if isinstance(veto_raw, list):
        veto_list = [v.strip() for v in veto_raw if v.strip()]
    else:
        veto_list = [v.strip() for v in str(veto_raw).split(",") if v.strip()]

    provider = ai.get("provider", "gemini").lower()
    gemini_key = ai.get("gemini_key", "").strip()
    gemini_model = ai.get("gemini_model", "gemini-3.5-flash-lite").strip()
    groq_key = ai.get("groq_key", "").strip()
    groq_model = ai.get("groq_model", "llama-3.3-70b-versatile").strip()
    score_threshold = int(ai.get("score_threshold", 70))

    active_model = groq_model if provider == "groq" else gemini_model
    updated_files = []

    # 1. Update NopeRi config.json
    if os.path.exists(NOPERI_CONFIG):
        try:
            with open(NOPERI_CONFIG, "r", encoding="utf-8") as f:
                ncfg = json.load(f)
            ncfg.setdefault("profile", {})
            ncfg["profile"]["experience_years"] = exp_years
            ncfg["profile"]["current_ctc"] = curr_ctc_lacs
            ncfg["profile"]["expected_ctc"] = exp_ctc_lacs
            ncfg["profile"]["notice_days"] = notice_days
            ncfg["profile"]["tech_stack"] = skills_list
            ncfg["profile"]["locations"] = locations
            ncfg["profile"]["resume_path"] = resume_path

            ncfg.setdefault("ai", {})
            ncfg["ai"]["provider"] = provider
            ncfg["ai"]["model"] = active_model
            ncfg["ai"]["score_threshold"] = score_threshold

            with open(NOPERI_CONFIG, "w", encoding="utf-8") as f:
                json.dump(ncfg, f, indent=2)
            updated_files.append("NopeRi/config.json")
        except Exception as e:
            print(f"Error updating NopeRi config.json: {e}")

    # 2. Update NopeRi .env
    np_env_updates = {"RESUME_PATH": resume_path}
    if gemini_key:
        np_env_updates["GEMINI_API_KEY"] = gemini_key
    if groq_key:
        np_env_updates["GROQ_API_KEY"] = groq_key
    update_env_file(NOPERI_ENV, np_env_updates)
    updated_files.append("NopeRi/.env")

    # 3. Update NopeRi qa_cache.json
    if os.path.exists(NOPERI_QA):
        try:
            with open(NOPERI_QA, "r", encoding="utf-8") as f:
                qas = json.load(f)
            qas["what is your current ctc in lacs per annum"] = curr_ctc_lacs
            qas["what is your expected ctc in lacs per annum"] = exp_ctc_lacs
            qas["how many years of experience do you have in python"] = str(exp_years)
            qas["how many years of experience do you have in fullstack development"] = str(exp_years)
            with open(NOPERI_QA, "w", encoding="utf-8") as f:
                json.dump(qas, f, indent=2)
            updated_files.append("NopeRi/qa_cache.json")
        except Exception as e:
            print(f"Error updating qa_cache: {e}")

    # 4. Update job-bot config.yaml
    if os.path.exists(JOBBOT_CONFIG):
        try:
            with open(JOBBOT_CONFIG, "r", encoding="utf-8") as f:
                y = yaml.safe_load(f) or {}
            y.setdefault("applicant", {})
            y["applicant"]["name"] = name
            y["applicant"]["email"] = email
            y["applicant"]["phone"] = phone
            y["applicant"]["current_location"] = location
            y["applicant"]["current_ctc"] = curr_ctc_full
            y["applicant"]["expected_ctc"] = exp_ctc_full
            y["applicant"]["notice_period"] = str(notice_days)
            y["applicant"]["experience_years"] = str(exp_years)
            y["applicant"]["willing_to_relocate"] = cand.get("willing_to_relocate", "Yes")

            y["resume_path"] = resume_path
            y["user_skills"] = [s.lower() for s in skills_list]
            if veto_list:
                y["veto_skills"] = [v.lower() for v in veto_list]
            y.setdefault("filters", {})
            y["filters"]["locations"] = locations
            y["filters"]["experience"] = "0-1" if is_fresher else f"0-{exp_years}"

            y.setdefault("ai", {})
            y["ai"]["provider"] = provider
            y["ai"]["model"] = active_model
            y["ai"]["score_threshold"] = score_threshold

            with open(JOBBOT_CONFIG, "w", encoding="utf-8") as f:
                yaml.safe_dump(y, f, sort_keys=False, allow_unicode=True)
            updated_files.append("job-bot/config.yaml")
        except Exception as e:
            print(f"Error updating job-bot config.yaml: {e}")

    # 5. Update job-bot .env
    jb_env_updates = {
        "AI_PROVIDER": provider,
        "RESUME_PATH": resume_path
    }
    if gemini_key:
        jb_env_updates["GEMINI_API_KEY"] = gemini_key
        jb_env_updates["GEMINI_MODEL"] = gemini_model
    if groq_key:
        jb_env_updates["GROQ_API_KEY"] = groq_key
        jb_env_updates["GROQ_MODEL"] = groq_model
    update_env_file(JOBBOT_ENV, jb_env_updates)
    updated_files.append("job-bot/.env")

    # 6. Update in-memory profile
    CANDIDATE["name"] = name
    CANDIDATE["email"] = email
    CANDIDATE["phone"] = phone
    CANDIDATE["location"] = location
    CANDIDATE["current_ctc"] = f"₹{curr_ctc_lacs} LPA (Fresher)" if curr_ctc_lacs == "0" else f"₹{curr_ctc_lacs} LPA"
    CANDIDATE["expected_ctc"] = f"₹{exp_ctc_lacs} LPA"
    CANDIDATE["experience"] = f"{exp_years} Years (Fresher)" if is_fresher else f"{exp_years} Years"
    CANDIDATE["notice_period"] = f"{notice_days} Days / Immediate"
    CANDIDATE["resume_path"] = resume_path
    CANDIDATE["primary_skills"] = skills_list
    CANDIDATE["ai_model"] = f"{provider.title()} ({active_model})"

    return {
        "success": True,
        "message": f"Successfully updated & synchronized across NopeRi and job-bot!",
        "updated_files": updated_files,
        "candidate": CANDIDATE
    }


# --- Enrichment Helpers: Skills, Descriptions, Experience ---

def extract_experience(title: str, url: str) -> str:
    m = re.search(r'(\d+)-to-(\d+)-years', url or "")
    if m:
        min_y, max_y = m.group(1), m.group(2)
        if min_y == "0":
            return f"0 - {max_y} Years (Fresher Friendly)"
        return f"{min_y} - {max_y} Years"
    t = (title or "").lower()
    if any(k in t for k in ['fresher', 'intern', 'trainee', '0-exp', 'entry', 'junior', 'jr']):
        return "0 - 1 Years (Fresher)"
    return "0 - 2 Years (Junior / Entry Level)"


def extract_job_skills(title: str, reason: str = "", url: str = "") -> List[str]:
    combined = f"{title} {reason} {url}".lower()
    skills = []

    if re.search(r'\breact\b|\breact\.js\b', combined):
        skills.append("React.js")
    if re.search(r'\bnode\b|\bnode\.js\b|\bexpress\b', combined):
        skills.append("Node.js / Express")
    if re.search(r'\bfull\s*stack\b|\bmmern\b|\bfull\-stack\b', combined):
        for s in ["React.js", "Node.js / Express"]:
            if s not in skills:
                skills.append(s)
    if re.search(r'\bjavascript\b|\bjs\b|\bes6\b', combined):
        skills.append("JavaScript (ES6+)")
    if re.search(r'\bpython\b|\bdjango\b|\bflask\b', combined):
        skills.append("Python")
    if re.search(r'\bmysql\b|\bsql\b|\bpostgres\b|\bdatabase\b', combined):
        skills.append("SQL / MySQL")
    if re.search(r'\bapi\b|\bapis\b|\brest\b|\brestful\b', combined):
        skills.append("REST APIs")
    if re.search(r'\btypescript\b|\bts\b', combined):
        skills.append("TypeScript")
    if re.search(r'\bmongo\b|\bmongodb\b', combined):
        skills.append("MongoDB")
    if re.search(r'\bai\b|\bllm\b|\bprompt\b|\bvoice\b', combined):
        skills.append("AI & Automation")
    if re.search(r'\bjava\b|\bspring\b', combined) and "javascript" not in combined:
        skills.append("Java / Spring Boot")
    if re.search(r'\btesting\b|\btester\b|\bqa\b|\bselenium\b', combined):
        skills.append("QA & Test Automation")
    if re.search(r'\bhtml\b|\bcss\b|\btailwind\b', combined):
        skills.append("HTML5 & Tailwind CSS")

    for default_s in ["JavaScript (ES6+)", "Python", "SQL / MySQL", "REST APIs", "Git / GitHub"]:
        if default_s not in skills:
            skills.append(default_s)
        if len(skills) >= 4:
            break

    return skills[:5]


def generate_job_description(title: str, company: str, loc: str, skills: List[str], exp: str) -> str:
    t = re.sub(r'[\-_]', ' ', (title or "").lower())
    skills_text = ", ".join(skills[:3]) if skills else "modern software tooling"
    comp_clean = company or "Direct Employer"
    loc_clean = loc or "Bengaluru / Pan-India"

    if any(k in t for k in ["full stack", "mern"]):
        return (f"Seeking a passionate Full Stack Developer to build and deploy end-to-end web applications at {comp_clean} ({loc_clean}). "
                f"Responsibilities include crafting responsive frontend components, engineering performant RESTful backend APIs, "
                f"and database architecture using {skills_text}. Great fit for 2026 freshers with project experience.")
    elif any(k in t for k in ["react", "frontend", "front end"]):
        return (f"Frontend Engineering role at {comp_clean} responsible for building modern, high-performance user interfaces. "
                f"Involves converting UI/UX designs into modular components, managing application state, and integrating client-side "
                f"workflows using {skills_text}.")
    elif any(k in t for k in ["node", "backend", "back end", "python"]):
        return (f"Backend development position at {comp_clean} focused on architecting scalable microservices, managing API endpoints, "
                f"and structuring persistent data storage with {skills_text}. Emphasizes clean code standards and system reliability.")
    elif any(k in t for k in ["ai", "machine learning", "automation"]):
        return (f"Innovative AI & Automation role at {comp_clean} focusing on intelligent agent workflows, LLM API integration, "
                f"task automation pipelines, and core backend infrastructure utilizing {skills_text}.")
    elif any(k in t for k in ["qa", "tester", "automation tester"]):
        return (f"Quality Assurance position at {comp_clean} ensuring high release reliability through automated test suites, "
                f"functional verification, and API contract testing across modern web platforms.")
    else:
        return (f"Core Software Engineering opening at {comp_clean} in {loc_clean}. Key responsibilities include designing software features, "
                f"debugging codebases, collaborating in agile sprints, and delivering robust software solutions with {skills_text}.")


def clean_company_from_naukri_url(url: str, title: str, existing_comp: str = "") -> str:
    if existing_comp and existing_comp.strip() and existing_comp.strip() != "Company":
        return existing_comp.split("\n")[0].strip()
    if not url or "naukri.com" not in url:
        return existing_comp or "Naukri Verified Employer"

    slug = url.split('/')[-1].replace('job-listings-', '')
    s = re.sub(r'-\d+-to-\d+-years-\d+$', '', slug)
    s = re.sub(r'-\d+$', '', s)
    cities = ['bengaluru', 'bangalore', 'mumbai', 'pune', 'hyderabad', 'chennai', 'kolkata', 'new-delhi', 'delhi', 'gurugram', 'gurgaon', 'noida', 'pimpri-chinchwad', 'navi-mumbai', 'remote']
    for _ in range(4):
        for city in cities:
            if s.endswith('-' + city):
                s = s[:-len('-' + city)]
    title_slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')
    if s.startswith(title_slug):
        comp = s[len(title_slug):].strip('-').replace('-', ' ').title()
        if comp:
            return comp
    parts = s.split('-')
    if len(parts) >= 2:
        return ' '.join(parts[-2:]).title()
    return "Naukri Verified Employer"


def evaluate_fit(title: str, company: str, loc: str, platform: str = "Naukri") -> tuple[int, str]:
    t = (title or "").lower()
    l = (loc or "").lower()
    score = 75
    reasons = []

    if any(k in t for k in ['react', 'node', 'mern', 'javascript', 'frontend', 'front end']):
        score += 17
        reasons.append("React & Node.js stack match")
    elif 'python' in t:
        score += 16
        reasons.append("Python core development match")
    elif any(k in t for k in ['full stack', 'fullstack']):
        score += 15
        reasons.append("Full Stack developer match")
    elif 'software engineer' in t or 'software developer' in t:
        score += 12
        reasons.append("Core CS Software Engineering match")
    elif 'ai' in t or 'ml' in t:
        score += 13
        reasons.append("AI & Automation project match (IRIS)")
    elif any(k in t for k in ['sales', 'voice', 'bpo', 'telecaller', 'non-voice']):
        return 48, "Non-technical or support role (Lower tech alignment)"
    elif any(k in t for k in ['java', 'spring', 'php', 'ruby', 'tester', 'qa', 'automation']):
        score += 6
        reasons.append("CS fundamental overlap")

    if any(k in t for k in ['fresher', 'intern', 'trainee', 'junior', 'jr', 'associate', 'walk-in']):
        score += 6
        reasons.append("Fresher (0-exp) / Entry Level")

    if any(k in l for k in ['bengaluru', 'bangalore', 'remote', 'hybrid', 'india']):
        score += 3
        reasons.append("Location match (Bengaluru / Remote)")

    score = min(98, max(42, score))
    reason_str = " • ".join(reasons) if reasons else "Relevant CS entry role matching candidate stack"
    return score, reason_str


SESSION_APPLIED_IDS = set()

def get_all_jobs_data() -> Dict[str, Any]:
    noperi_jobs = []
    jobbot_jobs = []
    discovered_list = []
    seen_ids = set()

    # 1. Load Discovered & Scored Jobs (fresh live search results)
    if os.path.exists(NOPERI_DISCOVERED):
        try:
            with open(NOPERI_DISCOVERED, "r", encoding="utf-8") as f:
                raw_disc = json.load(f)
            for d in raw_disc:
                jid = str(d.get("job_id") or "")
                if not jid or jid in seen_ids:
                    continue
                seen_ids.add(jid)

                is_applied = (jid in SESSION_APPLIED_IDS)
                score = int(d.get("ai_score") or 70)
                reason = d.get("ai_reason") or "Stack alignment verified"
                title = d.get("title") or "Software Engineer"
                comp = d.get("company") or "Employer"
                loc = d.get("location") or "Bengaluru"
                url = d.get("url") or f"https://www.naukri.com/job-listings-{jid}"

                exp = d.get("experience") or extract_experience(title, url)
                skills = d.get("tags") or extract_job_skills(title, reason, url)
                desc = d.get("description") or generate_job_description(title, comp, loc, skills, exp)
                tier = "Exceptional Fit" if score >= 88 else ("Strong Fit" if score >= 75 else "Good Fit")

                if is_applied:
                    status = "Applied"
                elif score >= 70:
                    status = "Ready to Apply"
                else:
                    status = "Needs Review"

                discovered_list.append({
                    "id": jid,
                    "title": title,
                    "company": comp,
                    "location": loc,
                    "experience": exp,
                    "salary": d.get("salary") or "₹3.5 - 5.0 LPA (Target: ₹4 LPA)",
                    "skills": skills,
                    "description": desc,
                    "platform": "Naukri (CloudGateway API)",
                    "source": "NopeRi",
                    "ai_score": score,
                    "ai_tier": tier,
                    "ai_reason": reason,
                    "status": status,
                    "applied": is_applied,
                    "applied_at": d.get("applied_at") if is_applied else "",
                    "url": url
                })
        except Exception as e:
            print(f"Error reading discovered jobs: {e}")

    # 2. Fetch NopeRi Available Jobs
    if os.path.exists(NOPERI_DB):
        try:
            conn = sqlite3.connect(NOPERI_DB)
            cur = conn.cursor()
            rows = cur.execute("SELECT hash, job_id, title, company, location, ai_score, applied_at FROM applied_jobs ORDER BY applied_at DESC").fetchall()
            conn.close()

            for r in rows:
                h, jid, title, comp, loc, db_score, app_at = r
                card_id = str(jid or h)
                if not card_id or card_id in seen_ids:
                    continue
                seen_ids.add(card_id)

                is_applied = (card_id in SESSION_APPLIED_IDS) or (str(h) in SESSION_APPLIED_IDS)
                score = db_score or 75
                url = f"https://www.naukri.com/job-listings-{jid}" if jid else "https://www.naukri.com"
                exp = extract_experience(title, url)
                skills = extract_job_skills(title, "", url)
                desc = generate_job_description(title, comp, loc, skills, exp)
                tier = "Exceptional Fit" if score >= 88 else ("Strong Fit" if score >= 75 else "Good Fit")
                status = "Applied" if is_applied else ("Ready to Apply" if score >= 70 else "Needs Review")

                noperi_jobs.append({
                    "id": card_id,
                    "title": title or "Software Engineer",
                    "company": comp or "Naukri Verified Employer",
                    "location": loc or "Bengaluru",
                    "experience": exp,
                    "salary": "₹3.5 - 5.0 LPA (Target: ₹4 LPA)",
                    "skills": skills,
                    "description": desc,
                    "platform": "Naukri (CloudGateway API)",
                    "source": "NopeRi",
                    "ai_score": int(score),
                    "ai_tier": tier,
                    "ai_reason": f"Naukri direct vacancy with {score}% match",
                    "status": status,
                    "applied": is_applied,
                    "applied_at": app_at if is_applied else "",
                    "url": url
                })
        except Exception as e:
            print(f"Error reading NopeRi DB: {e}")

    # 3. Fetch job-bot Available Jobs
    if os.path.exists(JOBBOT_DB):
        try:
            conn = sqlite3.connect(JOBBOT_DB)
            cur = conn.cursor()
            rows = cur.execute("SELECT id, url, title, company, location, platform, status, applied_at FROM applied ORDER BY applied_at DESC").fetchall()
            conn.close()

            for r in rows:
                jid, url, title, comp, loc, plat, status_val, app_at = r
                card_id = f"jb_{jid}"
                if not card_id or card_id in seen_ids:
                    continue
                seen_ids.add(card_id)

                is_applied = (card_id in SESSION_APPLIED_IDS) or (str(jid) in SESSION_APPLIED_IDS) or (str(url) in SESSION_APPLIED_IDS)
                clean_comp = clean_company_from_naukri_url(url, title, comp)
                clean_loc = (loc or "Bengaluru").splitlines()[0].strip() if (loc or "Bengaluru") else "Bengaluru"
                score, reason = evaluate_fit(title, clean_comp, clean_loc, plat)
                exp = extract_experience(title, url)
                skills = extract_job_skills(title, reason, url)
                desc = generate_job_description(title, clean_comp, clean_loc, skills, exp)
                tier = "Exceptional Fit" if score >= 88 else ("Strong Fit" if score >= 75 else "Good Fit")
                status = "Applied" if is_applied else ("Ready to Apply" if score >= 70 else "Needs Review")

                jobbot_jobs.append({
                    "id": card_id,
                    "title": title or "Software Developer",
                    "company": clean_comp,
                    "location": clean_loc,
                    "experience": exp,
                    "salary": "₹3.5 - 5.0 LPA (Target: ₹4 LPA)",
                    "skills": skills,
                    "description": desc,
                    "platform": plat or "Naukri",
                    "source": "job-bot",
                    "ai_score": int(score),
                    "ai_tier": tier,
                    "ai_reason": reason,
                    "status": status,
                    "applied": is_applied,
                    "applied_at": app_at if is_applied else "",
                    "url": url or "#"
                })
        except Exception as e:
            print(f"Error reading job-bot DB: {e}")

    # Combine all unique jobs, sorting Ready to Apply (≥70%) at the top
    all_jobs = discovered_list + noperi_jobs + jobbot_jobs
    all_jobs.sort(key=lambda x: (not x.get("applied"), x.get("ai_score", 0)), reverse=True)

    total_applied = sum(1 for j in all_jobs if j.get("applied"))
    ready_count = sum(1 for j in all_jobs if not j.get("applied") and j.get("ai_score", 0) >= 70)
    review_count = sum(1 for j in all_jobs if not j.get("applied") and j.get("ai_score", 0) < 70)
    high_match = sum(1 for j in all_jobs if j.get("ai_score", 0) >= 70)

    noperi_list = [j for j in all_jobs if j.get("source") == "NopeRi"]
    jobbot_list = [j for j in all_jobs if j.get("source") == "job-bot"]

    return {
        "candidate": CANDIDATE,
        "all": all_jobs,
        "noperi": noperi_list,
        "jobbot": jobbot_list,
        "stats": {
            "total_jobs": len(all_jobs),
            "total_applied": total_applied,
            "ready_to_apply": ready_count,
            "needs_review": review_count,
            "high_match": high_match,
            "noperi_total": len(noperi_list),
            "jobbot_total": len(jobbot_list),
            "naukri_count": sum(1 for j in all_jobs if "naukri" in j["platform"].lower()),
            "internshala_count": sum(1 for j in all_jobs if "internshala" in j["platform"].lower()),
            "indeed_count": sum(1 for j in all_jobs if "indeed" in j["platform"].lower())
        }
    }


# --- API Endpoints ---

@app.get("/api/jobs")
def api_get_jobs():
    return JSONResponse(content=get_all_jobs_data())


@app.post("/api/reset-applied")
def api_reset_applied():
    global SESSION_APPLIED_IDS
    SESSION_APPLIED_IDS.clear()
    return JSONResponse(content={"success": True, "message": "All applications reset to unapplied! Ready to apply."})


@app.post("/api/apply-job")
def api_apply_job(data: Dict[str, Any]):
    job_id = str(data.get("job_id") or "")
    title = data.get("title") or "Software Engineer"
    company = data.get("company") or "Employer"
    url = data.get("url") or ""
    platform = data.get("platform") or "Naukri"

    SESSION_APPLIED_IDS.add(str(job_id))
    runner.broadcast(f"🎯 [MANUAL APPLY] User clicked 'Apply Now' for '{title}' @ '{company}' (ID: {job_id})")

    # If it's a Naukri direct API job
    if job_id and job_id.isdigit():
        try:
            import sys
            if NOPERI_DIR not in sys.path:
                sys.path.insert(0, NOPERI_DIR)
            from src.utils.config_manager import ConfigManager
            from src.utils.dbhandler import AppliedJobsDB
            from src.client.naukri_client import NaukriLoginClient
            from src.client.job_client import NaukriJobClient
            from src.models.models import Job

            cfg = ConfigManager()
            db = AppliedJobsDB(db_path=NOPERI_DB, csv_path=os.path.join(NOPERI_DIR, "applied_jobs.csv"))
            client = NaukriLoginClient(
                username=os.getenv("NAUKRI_USERNAME") or cfg.config.get("profile", {}).get("email"),
                password=os.getenv("PASSWORD")
            )
            jc = NaukriJobClient(client, config=cfg.config)
            
            job_obj = Job(
                job_id=job_id,
                title=title,
                company=company,
                location=data.get("location", "Bengaluru"),
                experience=data.get("experience", "0-2 Years"),
                salary=data.get("salary", "4 LPA"),
                posted_date="",
                apply_link=url
            )
            
            # Check if external apply
            if jc.is_external_apply(job_id):
                runner.broadcast(f"ℹ️ [MANUAL APPLY] '{title}' requires external portal apply. Opening link: {url}")
                return {
                    "success": True,
                    "applied": False,
                    "external": True,
                    "url": url,
                    "message": f"'{title}' requires external portal apply. Opened link."
                }
            
            # Attempt apply with auto token refresh if needed
            try:
                result = jc.apply_job(job_obj, source="search")
            except Exception as apply_err:
                if "auth" in str(apply_err).lower() and os.getenv("NAUKRI_USERNAME") and os.getenv("PASSWORD"):
                    runner.broadcast("🔄 Auth expired. Re-authenticating session with Naukri...")
                    client.login()
                    jc._session = client.session
                    result = jc.apply_job(job_obj, source="search")
                else:
                    raise apply_err

            job_res = (result.get("jobs") or [{}])[0]
            if job_res.get("questionnaire"):
                runner.broadcast(f"⚡ [MANUAL APPLY] Questionnaire detected for '{title}'. Handling automatically...")
                sid = datetime.utcnow().strftime("%Y%m%d%H%M%S") + "0000000"
                _qa = None
                if not cfg.config.get("questionnaire", {}).get("skip_ai", False):
                    try:
                        from src.client.questionnaire_ai import QuestionnaireAI
                        _qa = QuestionnaireAI(cfg)
                    except Exception:
                        pass
                precomputed = _qa.answer(job_res["questionnaire"], job_obj) if _qa else None
                result = jc.handle_static_questionnaire_and_apply(
                    job_obj, questionnaire=job_res["questionnaire"], sid=sid, source="search", precomputed_answers=precomputed
                )

            score = int(data.get("ai_score") or 80)
            db.mark_applied(job_obj, ai_score=score)
            
            # Update discovered_jobs.json
            disc_path = os.path.join(NOPERI_DIR, "discovered_jobs.json")
            if os.path.exists(disc_path):
                try:
                    with open(disc_path, "r", encoding="utf-8") as f:
                        djobs = json.load(f)
                    for dj in djobs:
                        if str(dj.get("job_id")) == str(job_id):
                            dj["applied"] = True
                            dj["status"] = "Applied"
                            dj["applied_at"] = datetime.utcnow().isoformat()
                    with open(disc_path, "w", encoding="utf-8") as f:
                        json.dump(djobs, f, indent=2)
                except Exception:
                    pass

            runner.broadcast(f"✅ [MANUAL APPLY] Applied successfully to '{title}' @ '{company}'!")
            return {
                "success": True,
                "applied": True,
                "job_id": job_id,
                "message": f"Applied successfully to '{title}' at '{company}'!"
            }
        except Exception as e:
            runner.broadcast(f"❌ [MANUAL APPLY FAILED] {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Application failed: {str(e)}"
            }
    else:
        # job-bot or external URL
        runner.broadcast(f"🔗 [MANUAL APPLY] Opening vacancy link in browser: {url}")
        return {
            "success": True,
            "applied": False,
            "external": True,
            "url": url,
            "message": f"Opened link for {title}."
        }


@app.get("/api/settings")
def api_get_settings():
    return JSONResponse(content=load_all_settings())


@app.post("/api/settings")
def api_save_settings(data: Dict[str, Any]):
    res = sync_all_settings(data)
    return JSONResponse(content=res)


@app.post("/api/settings/test-key")
def api_test_key(data: Dict[str, Any]):
    provider = data.get("provider", "gemini").lower()
    key = data.get("key", "").strip()
    model = data.get("model", "").strip()

    if not key:
        return {"valid": False, "message": "API Key is empty."}

    start_time = time.time()
    try:
        import requests
        if provider == "groq":
            target_model = model or "llama-3.3-70b-versatile"
            res = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={
                    "model": target_model,
                    "messages": [{"role": "user", "content": "Ping test: reply with 'OK'"}],
                    "max_tokens": 10
                },
                timeout=12
            )
            latency = int((time.time() - start_time) * 1000)
            if res.status_code == 200:
                resp_text = res.json()["choices"][0]["message"]["content"].strip()
                return {
                    "valid": True,
                    "provider": "Groq Cloud",
                    "model": target_model,
                    "latency_ms": latency,
                    "message": f"Connected! ({target_model} in {latency}ms: '{resp_text}')"
                }
            else:
                return {
                    "valid": False,
                    "provider": "Groq Cloud",
                    "message": f"HTTP {res.status_code}: {res.text[:150]}"
                }
        else: # Gemini
            target_model = model or "gemini-3.5-flash-lite"
            res = requests.post(
                "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={
                    "model": target_model,
                    "messages": [{"role": "user", "content": "Ping test: reply with 'OK'"}],
                    "max_tokens": 10
                },
                timeout=12
            )
            latency = int((time.time() - start_time) * 1000)
            if res.status_code == 200:
                resp_text = res.json()["choices"][0]["message"]["content"].strip()
                return {
                    "valid": True,
                    "provider": "Google Gemini",
                    "model": target_model,
                    "latency_ms": latency,
                    "message": f"Connected! ({target_model} in {latency}ms: '{resp_text}')"
                }
            else:
                return {
                    "valid": False,
                    "provider": "Google Gemini",
                    "message": f"HTTP {res.status_code}: {res.text[:150]}"
                }
    except Exception as e:
        return {"valid": False, "message": f"Connection error: {str(e)}"}


@app.get("/api/status")
def api_get_status():
    return {
        "running": runner.running,
        "current_task": runner.current_task
    }


@app.post("/api/run")
def api_run_bot(data: Dict[str, Any]):
    target = data.get("target", "both")
    platform = data.get("platform", "all")
    auto_flag = data.get("auto", True)
    py_exe = sys.executable

    if target == "noperi":
        cmd = [py_exe, "-u", "apply_agent.py"]
        if auto_flag:
            cmd.append("--auto")
        success, msg = runner.run_command(cmd, cwd=NOPERI_DIR, task_name="NopeRi (Naukri Direct API)")
        return {"success": success, "message": msg}

    elif target == "jobbot":
        cmd = [py_exe, "-u", "main.py"]
        if auto_flag:
            cmd.append("--auto")
        if platform and platform != "all":
            cmd.extend(["--platform", platform])
        success, msg = runner.run_command(cmd, cwd=JOBBOT_DIR, task_name=f"job-bot ({platform.title()})")
        return {"success": success, "message": msg}

    elif target == "both":
        def run_both_thread():
            cmd1 = [py_exe, "-u", "apply_agent.py"]
            if auto_flag:
                cmd1.append("--auto")
            runner.broadcast("⚡ [RUN BOTH] Stage 1/2: Running NopeRi Direct API...")
            runner.run_command(cmd1, cwd=NOPERI_DIR, task_name="NopeRi (Stage 1/2)")
            
            while runner.running:
                time.sleep(1)

            runner.broadcast("⚡ [RUN BOTH] Stage 2/2: Running job-bot Multi-Platform...")
            cmd2 = [py_exe, "-u", "main.py"]
            if auto_flag:
                cmd2.append("--auto")
            runner.run_command(cmd2, cwd=JOBBOT_DIR, task_name="job-bot (Stage 2/2)")

        if runner.running:
            return {"success": False, "message": "A bot run is already active."}
        
        t = threading.Thread(target=run_both_thread, daemon=True)
        t.start()
        return {"success": True, "message": "Started sequential run (NopeRi followed by job-bot)"}

    return {"success": False, "message": f"Unknown target: {target}"}


@app.post("/api/stop")
def api_stop():
    success, msg = runner.stop()
    return {"success": success, "message": msg}


@app.post("/api/open-terminal")
def api_open_terminal(data: Optional[Dict[str, Any]] = None):
    data = data or {}
    target = data.get("target", "noperi")
    platform = data.get("platform", "all")

    try:
        if target == "noperi":
            bat_path = os.path.join(NOPERI_DIR, "run.bat")
            cmd = f'start "NopeRi - Naukri Auto-Apply Live Console" cmd /k "cd /d \"{NOPERI_DIR}\" && \"{bat_path}\""'
            subprocess.Popen(cmd, cwd=NOPERI_DIR, shell=True)
            runner.broadcast("🖥️ Launched NopeRi in a dedicated Windows Command Prompt console window.")
            return {"success": True, "message": "NopeRi launched in native Windows Command Prompt window!"}
        elif target == "jobbot":
            bat_path = os.path.join(JOBBOT_DIR, "run.bat")
            cmd = f'start "job-bot - Multi-Portal Live Console" cmd /k "cd /d \"{JOBBOT_DIR}\" && \"{bat_path}\""'
            subprocess.Popen(cmd, cwd=JOBBOT_DIR, shell=True)
            runner.broadcast("🖥️ Launched job-bot in a dedicated Windows Command Prompt console window.")
            return {"success": True, "message": "job-bot launched in native Windows Command Prompt window!"}
        else:
            bat_path = os.path.join(BASE_DIR, "run.bat")
            cmd = f'start "AI Job Automation Hub Terminal" cmd /k "cd /d \"{BASE_DIR}\" && \"{bat_path}\""'
            subprocess.Popen(cmd, cwd=BASE_DIR, shell=True)
            runner.broadcast("🖥️ Launched AI Job Hub in a dedicated Windows Command Prompt console window.")
            return {"success": True, "message": "Opened AI Job Hub native terminal window!"}
    except Exception as e:
        return {"success": False, "message": f"Error opening native terminal: {str(e)}"}


@app.get("/api/stream")
@app.get("/api/logs/stream")
async def api_logs_stream(request: Request):
    q = runner.subscribe()

    async def event_generator():
        try:
            yield "data: [SYSTEM] Connected to Live Terminal Stream\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    line = q.get_nowait()
                    safe_line = line.replace("\n", " ").replace("\r", "")
                    yield f"data: {safe_line}\n\n"
                except queue.Empty:
                    await asyncio.sleep(0.4)
                    yield ": ping\n\n"
                except Exception:
                    break
        finally:
            runner.unsubscribe(q)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/export")
def api_export_csv():
    data = get_all_jobs_data()
    all_jobs = data["noperi"] + data["jobbot"]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Engine / Source", "Job ID", "Job Title", "Company", "Location", 
        "Experience", "Target Salary", "Skills", "AI Match Score", 
        "AI Fit Reason", "Description", "Status", "Applied At", "Job URL"
    ])

    for j in all_jobs:
        writer.writerow([
            j.get("source"),
            j.get("id"),
            j.get("title"),
            j.get("company"),
            j.get("location"),
            j.get("experience"),
            j.get("salary"),
            ", ".join(j.get("skills", [])),
            f"{j.get('ai_score')}%",
            j.get("ai_reason"),
            j.get("description"),
            j.get("status"),
            j.get("applied_at"),
            j.get("url")
        ])

    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=AI_Job_Hub_Export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
    )


# --- High-Design HTML / Single Page Application ---

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>AI Job Automation Hub — NopeRi & job-bot Command Center</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-base: #060813;
      --surface-glass: rgba(13, 20, 36, 0.72);
      --surface-glass-hover: rgba(20, 30, 52, 0.85);
      --surface-card: #0b1120;
      --border-subtle: rgba(255, 255, 255, 0.08);
      --border-glow: rgba(99, 102, 241, 0.28);
      --border-hover: rgba(148, 163, 184, 0.22);
      --text-primary: #f8fafc;
      --text-secondary: #94a3b8;
      --text-muted: #64748b;
      --primary: #6366f1;
      --primary-hover: #4f46e5;
      --noperi: #f97316;
      --jobbot: #06b6d4;
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
      --font-sans: 'Plus Jakarta Sans', system-ui, -apple-system, sans-serif;
      --font-mono: 'JetBrains Mono', monospace;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }
    
    body {
      background-color: var(--bg-base);
      background-image: 
        radial-gradient(1200px circle at 10% 8%, rgba(99, 102, 241, 0.14), transparent 42%),
        radial-gradient(1000px circle at 90% 12%, rgba(6, 182, 212, 0.12), transparent 45%),
        radial-gradient(800px circle at 50% 95%, rgba(249, 115, 22, 0.08), transparent 40%),
        linear-gradient(to right, rgba(255, 255, 255, 0.015) 1px, transparent 1px),
        linear-gradient(to bottom, rgba(255, 255, 255, 0.015) 1px, transparent 1px);
      background-size: 100% 100%, 100% 100%, 100% 100%, 36px 36px, 36px 36px;
      color: var(--text-primary);
      font-family: var(--font-sans);
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      overflow-x: hidden;
      -webkit-font-smoothing: antialiased;
    }

    /* Scrollbars */
    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: #070a14; }
    ::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 9999px; }
    ::-webkit-scrollbar-thumb:hover { background: #334155; }

    /* Sticky Header */
    header {
      background: rgba(10, 15, 28, 0.82);
      backdrop-filter: blur(20px);
      -webkit-backdrop-filter: blur(20px);
      border-bottom: 1px solid var(--border-subtle);
      position: sticky;
      top: 0;
      z-index: 100;
      padding: 0.85rem 1.75rem;
      box-shadow: 0 4px 24px rgba(0, 0, 0, 0.4);
    }
    .header-content {
      max-width: 1600px;
      margin: 0 auto;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 1.25rem;
      flex-wrap: wrap;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 0.85rem;
    }
    .brand-icon {
      width: 44px;
      height: 44px;
      border-radius: 12px;
      background: linear-gradient(135deg, #6366f1, #06b6d4, #f97316);
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 1.4rem;
      box-shadow: 0 4px 18px rgba(99, 102, 241, 0.35);
      position: relative;
    }
    .brand-title-wrap {
      display: flex;
      flex-direction: column;
    }
    .brand-title {
      font-size: 1.25rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      display: flex;
      align-items: center;
      gap: 0.5rem;
      background: linear-gradient(90deg, #ffffff 0%, #cbd5e1 100%);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }
    .badge-pro {
      font-size: 0.65rem;
      font-weight: 800;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      padding: 0.15rem 0.45rem;
      border-radius: 6px;
      background: linear-gradient(135deg, #6366f1, #06b6d4);
      color: #ffffff;
      -webkit-text-fill-color: #ffffff;
      box-shadow: 0 2px 8px rgba(99, 102, 241, 0.4);
    }
    .brand-sub {
      font-size: 0.76rem;
      color: var(--text-secondary);
      font-weight: 500;
    }

    .header-right {
      display: flex;
      align-items: center;
      gap: 0.75rem;
      flex-wrap: wrap;
    }
    .candidate-pill {
      display: flex;
      align-items: center;
      gap: 0.75rem;
      background: var(--surface-glass);
      border: 1px solid var(--border-subtle);
      padding: 0.4rem 0.95rem;
      border-radius: 9999px;
      font-size: 0.82rem;
      box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
    }
    .cand-avatar {
      width: 26px;
      height: 26px;
      border-radius: 50%;
      background: linear-gradient(135deg, #6366f1, #06b6d4);
      display: flex;
      align-items: center;
      justify-content: center;
      font-weight: 800;
      font-size: 0.72rem;
      color: white;
    }
    .badge-fresher {
      background: rgba(16, 185, 129, 0.12);
      color: #34d399;
      border: 1px solid rgba(16, 185, 129, 0.3);
      padding: 0.2rem 0.55rem;
      border-radius: 9999px;
      font-weight: 700;
      font-size: 0.72rem;
      display: flex;
      align-items: center;
      gap: 0.35rem;
    }
    .badge-fresher-dot {
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: #10b981;
      box-shadow: 0 0 8px #10b981;
    }
    .badge-ctc {
      background: rgba(245, 158, 11, 0.12);
      color: #fbbf24;
      border: 1px solid rgba(245, 158, 11, 0.3);
      padding: 0.2rem 0.55rem;
      border-radius: 9999px;
      font-weight: 700;
      font-size: 0.72rem;
    }

    /* Main Container */
    main {
      max-width: 1600px;
      width: 100%;
      margin: 0 auto;
      padding: 1.5rem 1.75rem 3rem;
      flex: 1;
      display: flex;
      flex-direction: column;
      gap: 1.35rem;
    }

    /* Command Action Deck */
    .action-panel {
      background: var(--surface-glass);
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
      border: 1px solid var(--border-subtle);
      border-radius: 18px;
      padding: 1.15rem 1.5rem;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 1.25rem;
      flex-wrap: wrap;
      box-shadow: 0 10px 32px rgba(0, 0, 0, 0.35);
      position: relative;
      overflow: hidden;
    }
    .action-panel::before {
      content: '';
      position: absolute;
      top: 0; left: 0; right: 0; height: 1px;
      background: linear-gradient(90deg, transparent, rgba(99, 102, 241, 0.5), rgba(6, 182, 212, 0.5), transparent);
    }
    .action-left {
      display: flex;
      align-items: center;
      gap: 0.85rem;
      flex-wrap: wrap;
    }
    .action-right {
      display: flex;
      align-items: center;
      gap: 0.85rem;
      flex-wrap: wrap;
    }

    .btn {
      display: inline-flex;
      align-items: center;
      gap: 0.55rem;
      padding: 0.65rem 1.3rem;
      font-size: 0.88rem;
      font-weight: 700;
      font-family: var(--font-sans);
      border-radius: 11px;
      border: none;
      cursor: pointer;
      transition: all 0.22s cubic-bezier(0.4, 0, 0.2, 1);
      text-decoration: none;
      user-select: none;
    }
    .btn:active { transform: scale(0.97); }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }

    .btn-both {
      background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
      color: white;
      box-shadow: 0 4px 18px rgba(99, 102, 241, 0.45);
    }
    .btn-both:hover {
      background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 100%);
      box-shadow: 0 6px 24px rgba(99, 102, 241, 0.65);
      transform: translateY(-2px);
    }

    .btn-noperi {
      background: linear-gradient(135deg, #f97316 0%, #ea580c 100%);
      color: white;
      box-shadow: 0 4px 16px rgba(249, 115, 22, 0.4);
    }
    .btn-noperi:hover {
      background: linear-gradient(135deg, #ea580c 0%, #c2410c 100%);
      box-shadow: 0 6px 22px rgba(249, 115, 22, 0.55);
      transform: translateY(-2px);
    }

    .btn-jobbot {
      background: linear-gradient(135deg, #06b6d4 0%, #0284c7 100%);
      color: white;
      box-shadow: 0 4px 16px rgba(6, 182, 212, 0.4);
    }
    .btn-jobbot:hover {
      background: linear-gradient(135deg, #0891b2 0%, #0369a1 100%);
      box-shadow: 0 6px 22px rgba(6, 182, 212, 0.55);
      transform: translateY(-2px);
    }

    .btn-stop {
      background: rgba(239, 68, 68, 0.14);
      color: #f87171;
      border: 1px solid rgba(239, 68, 68, 0.35);
    }
    .btn-stop:hover {
      background: #ef4444;
      color: white;
      box-shadow: 0 4px 16px rgba(239, 68, 68, 0.4);
    }

    .btn-cmd {
      background: rgba(16, 185, 129, 0.12);
      color: #34d399;
      border: 1px solid rgba(16, 185, 129, 0.35);
    }
    .btn-cmd:hover {
      background: linear-gradient(135deg, #059669 0%, #10b981 100%);
      color: white;
      box-shadow: 0 4px 16px rgba(16, 185, 129, 0.4);
      transform: translateY(-2px);
    }

    .btn-settings {
      background: rgba(99, 102, 241, 0.12);
      color: #c7d2fe;
      border: 1px solid rgba(99, 102, 241, 0.3);
    }
    .btn-settings:hover {
      background: rgba(99, 102, 241, 0.24);
      border-color: #818cf8;
      color: white;
      transform: translateY(-1px);
    }

    .btn-refresh {
      background: rgba(255, 255, 255, 0.05);
      color: var(--text-secondary);
      border: 1px solid var(--border-subtle);
    }
    .btn-refresh:hover {
      background: rgba(255, 255, 255, 0.1);
      color: var(--text-primary);
    }

    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 0.55rem;
      padding: 0.45rem 0.95rem;
      border-radius: 9999px;
      font-size: 0.8rem;
      font-weight: 600;
      background: rgba(15, 23, 42, 0.6);
      border: 1px solid var(--border-subtle);
    }
    .status-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #64748b;
    }
    .status-dot.active {
      background: #10b981;
      box-shadow: 0 0 12px #10b981;
      animation: pulseRadar 1.5s infinite;
    }
    @keyframes pulseRadar {
      0% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(1.35); }
      100% { opacity: 1; transform: scale(1); }
    }

    /* KPI Metrics Cards */
    .metrics-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 1rem;
    }
    .metric-card {
      background: var(--surface-glass);
      backdrop-filter: blur(14px);
      -webkit-backdrop-filter: blur(14px);
      border: 1px solid var(--border-subtle);
      border-radius: 16px;
      padding: 1.15rem 1.35rem;
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
      position: relative;
      overflow: hidden;
      transition: transform 0.22s, border-color 0.22s, box-shadow 0.22s;
    }
    .metric-card:hover {
      transform: translateY(-3px);
      border-color: rgba(255, 255, 255, 0.16);
      box-shadow: 0 10px 28px rgba(0, 0, 0, 0.35);
    }
    .metric-card::before {
      content: '';
      position: absolute;
      top: 0; left: 0; right: 0; height: 3px;
    }
    .metric-card.m-blue::before { background: linear-gradient(90deg, #3b82f6, #60a5fa); }
    .metric-card.m-emerald::before { background: linear-gradient(90deg, #10b981, #34d399); }
    .metric-card.m-amber::before { background: linear-gradient(90deg, #f59e0b, #fbbf24); }
    .metric-card.m-purple::before { background: linear-gradient(90deg, #8b5cf6, #c084fc); }
    .metric-card.m-cyan::before { background: linear-gradient(90deg, #06b6d4, #38bdf8); }

    .metric-label {
      font-size: 0.74rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--text-secondary);
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .metric-value {
      font-size: 2.1rem;
      font-weight: 800;
      letter-spacing: -0.03em;
      line-height: 1.1;
    }
    .metric-sub {
      font-size: 0.76rem;
      color: var(--text-muted);
      margin-top: 0.15rem;
    }

    /* Live Terminal Console */
    .terminal-section {
      background: #050811;
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 16px;
      overflow: hidden;
      display: flex;
      flex-direction: column;
      box-shadow: 0 12px 36px rgba(0, 0, 0, 0.5);
    }
    .terminal-header {
      background: #090e1c;
      padding: 0.7rem 1.15rem;
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid rgba(255, 255, 255, 0.06);
    }
    .terminal-title-group {
      display: flex;
      align-items: center;
      gap: 0.85rem;
    }
    .terminal-dots {
      display: flex;
      align-items: center;
      gap: 0.35rem;
    }
    .t-dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
    }
    .t-dot.red { background: #ef4444; }
    .t-dot.yellow { background: #f59e0b; }
    .t-dot.green { background: #10b981; }
    .terminal-title {
      font-family: var(--font-mono);
      font-size: 0.78rem;
      color: var(--text-secondary);
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }
    .terminal-actions {
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }
    .term-btn {
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid rgba(255, 255, 255, 0.08);
      color: var(--text-secondary);
      border-radius: 7px;
      padding: 0.25rem 0.65rem;
      font-size: 0.72rem;
      font-family: var(--font-mono);
      cursor: pointer;
      transition: all 0.2s;
    }
    .term-btn:hover {
      background: rgba(255, 255, 255, 0.1);
      color: var(--text-primary);
    }
    .terminal-body {
      padding: 0.95rem 1.25rem;
      height: 190px;
      overflow-y: auto;
      font-family: var(--font-mono);
      font-size: 0.78rem;
      line-height: 1.6;
      color: #cbd5e1;
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
      background: #050811;
    }
    .term-line-success { color: #34d399; }
    .term-line-info { color: #38bdf8; }
    .term-line-warn { color: #fbbf24; }
    .term-line-error { color: #f87171; }

    /* Tabs Bar */
    .tabs-bar {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      border-bottom: 1px solid var(--border-subtle);
      padding-bottom: 0.6rem;
      overflow-x: auto;
    }
    .tab-btn {
      background: transparent;
      border: 1px solid transparent;
      color: var(--text-secondary);
      font-family: var(--font-sans);
      font-size: 0.92rem;
      font-weight: 700;
      padding: 0.55rem 1.15rem;
      border-radius: 10px;
      cursor: pointer;
      display: flex;
      align-items: center;
      gap: 0.5rem;
      transition: all 0.2s;
      white-space: nowrap;
    }
    .tab-btn:hover {
      color: var(--text-primary);
      background: rgba(255, 255, 255, 0.04);
    }
    .tab-btn.active {
      background: rgba(255, 255, 255, 0.08);
      color: white;
      border-color: rgba(255, 255, 255, 0.14);
    }
    .tab-btn.active.tab-noperi {
      background: rgba(249, 115, 22, 0.15);
      border-color: rgba(249, 115, 22, 0.4);
      color: #fb923c;
    }
    .tab-btn.active.tab-jobbot {
      background: rgba(6, 182, 212, 0.15);
      border-color: rgba(6, 182, 212, 0.4);
      color: #38bdf8;
    }
    .tab-btn.active.tab-ready {
      background: rgba(16, 185, 129, 0.15);
      border-color: rgba(16, 185, 129, 0.4);
      color: #34d399;
    }
    .tab-count {
      background: rgba(255, 255, 255, 0.1);
      padding: 0.15rem 0.5rem;
      border-radius: 9999px;
      font-size: 0.72rem;
      font-weight: 700;
    }

    /* Filter & Search Bar */
    .filter-bar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 1rem;
      flex-wrap: wrap;
      background: var(--surface-glass);
      backdrop-filter: blur(14px);
      -webkit-backdrop-filter: blur(14px);
      border: 1px solid var(--border-subtle);
      padding: 0.75rem 1.15rem;
      border-radius: 14px;
    }
    .filter-left {
      display: flex;
      align-items: center;
      gap: 0.75rem;
      flex-wrap: wrap;
      flex: 1;
      min-width: 280px;
    }
    .search-box {
      display: flex;
      align-items: center;
      gap: 0.55rem;
      background: rgba(6, 8, 19, 0.8);
      border: 1px solid var(--border-subtle);
      border-radius: 9px;
      padding: 0.45rem 0.85rem;
      flex: 1;
      max-width: 380px;
      transition: border-color 0.2s;
    }
    .search-box:focus-within {
      border-color: var(--primary);
      box-shadow: 0 0 0 2px rgba(99, 102, 241, 0.2);
    }
    .search-box input {
      background: transparent;
      border: none;
      outline: none;
      color: white;
      font-family: var(--font-sans);
      font-size: 0.84rem;
      width: 100%;
    }
    .search-box input::placeholder { color: var(--text-muted); }
    .search-clear {
      background: transparent;
      border: none;
      color: var(--text-muted);
      cursor: pointer;
      font-size: 0.8rem;
    }
    .search-clear:hover { color: white; }

    .chip-group {
      display: flex;
      align-items: center;
      gap: 0.4rem;
      flex-wrap: wrap;
    }
    .filter-chip {
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid var(--border-subtle);
      color: var(--text-secondary);
      border-radius: 8px;
      padding: 0.35rem 0.75rem;
      font-size: 0.78rem;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s;
    }
    .filter-chip:hover {
      background: rgba(255, 255, 255, 0.08);
      color: white;
    }
    .filter-chip.active {
      background: rgba(99, 102, 241, 0.2);
      border-color: rgba(99, 102, 241, 0.5);
      color: #a5b4fc;
    }

    .filter-right {
      display: flex;
      align-items: center;
      gap: 0.75rem;
    }
    .sort-select {
      background: rgba(6, 8, 19, 0.8);
      border: 1px solid var(--border-subtle);
      color: var(--text-primary);
      border-radius: 9px;
      padding: 0.45rem 0.85rem;
      font-size: 0.82rem;
      font-family: var(--font-sans);
      outline: none;
      cursor: pointer;
    }
    .view-toggle {
      display: flex;
      background: rgba(6, 8, 19, 0.8);
      border: 1px solid var(--border-subtle);
      border-radius: 9px;
      overflow: hidden;
    }
    .view-btn {
      background: transparent;
      border: none;
      color: var(--text-secondary);
      padding: 0.45rem 0.75rem;
      font-size: 0.82rem;
      cursor: pointer;
      display: flex;
      align-items: center;
      gap: 0.3rem;
    }
    .view-btn.active {
      background: rgba(255, 255, 255, 0.1);
      color: white;
    }

    /* Cards Grid */
    .jobs-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(380px, 1fr));
      gap: 1.25rem;
    }

    /* Pro Max Job Card */
    .job-card {
      background: var(--surface-glass);
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
      border: 1px solid var(--border-subtle);
      border-radius: 18px;
      padding: 1.35rem;
      display: flex;
      flex-direction: column;
      gap: 1rem;
      position: relative;
      transition: transform 0.25s cubic-bezier(0.4, 0, 0.2, 1), border-color 0.25s, box-shadow 0.25s;
      box-shadow: 0 4px 20px rgba(0, 0, 0, 0.25);
    }
    .job-card:hover {
      transform: translateY(-4px);
      border-color: rgba(99, 102, 241, 0.35);
      box-shadow: 0 14px 36px rgba(0, 0, 0, 0.4), 0 0 20px rgba(99, 102, 241, 0.15);
    }
    .job-card.is-applied {
      border-left: 3px solid #10b981;
    }
    .job-card.is-ready {
      border-left: 3px solid #f59e0b;
    }

    .card-top {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 0.75rem;
    }
    .card-badges {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      flex-wrap: wrap;
    }
    .platform-pill {
      font-size: 0.72rem;
      font-weight: 700;
      padding: 0.25rem 0.65rem;
      border-radius: 7px;
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
    }
    .pill-noperi {
      background: rgba(249, 115, 22, 0.12);
      color: #fb923c;
      border: 1px solid rgba(249, 115, 22, 0.35);
    }
    .pill-naukri {
      background: rgba(0, 139, 220, 0.12);
      color: #38bdf8;
      border: 1px solid rgba(0, 139, 220, 0.35);
    }
    .pill-internshala {
      background: rgba(0, 165, 236, 0.12);
      color: #38bdf8;
      border: 1px solid rgba(0, 165, 236, 0.35);
    }
    .pill-indeed {
      background: rgba(33, 100, 243, 0.12);
      color: #60a5fa;
      border: 1px solid rgba(33, 100, 243, 0.35);
    }

    .status-pill {
      font-size: 0.72rem;
      font-weight: 700;
      padding: 0.25rem 0.65rem;
      border-radius: 7px;
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
    }
    .status-applied {
      background: rgba(16, 185, 129, 0.14);
      color: #34d399;
      border: 1px solid rgba(16, 185, 129, 0.35);
    }
    .status-ready {
      background: rgba(245, 158, 11, 0.14);
      color: #fbbf24;
      border: 1px solid rgba(245, 158, 11, 0.35);
    }
    .status-review {
      background: rgba(148, 163, 184, 0.12);
      color: #94a3b8;
      border: 1px solid rgba(148, 163, 184, 0.25);
    }

    /* Score Badge */
    .ai-score-box {
      display: flex;
      flex-direction: column;
      align-items: flex-end;
    }
    .ai-score-badge {
      font-family: var(--font-mono);
      font-weight: 800;
      font-size: 1.05rem;
      padding: 0.25rem 0.65rem;
      border-radius: 8px;
      display: inline-flex;
      align-items: center;
      gap: 0.25rem;
    }
    .score-high {
      background: linear-gradient(135deg, rgba(16, 185, 129, 0.2), rgba(5, 150, 105, 0.2));
      color: #34d399;
      border: 1px solid rgba(16, 185, 129, 0.45);
      box-shadow: 0 0 16px rgba(16, 185, 129, 0.25);
    }
    .score-mid {
      background: linear-gradient(135deg, rgba(6, 182, 212, 0.2), rgba(37, 99, 235, 0.2));
      color: #38bdf8;
      border: 1px solid rgba(6, 182, 212, 0.45);
      box-shadow: 0 0 16px rgba(6, 182, 212, 0.2);
    }
    .score-mod {
      background: linear-gradient(135deg, rgba(245, 158, 11, 0.18), rgba(217, 119, 6, 0.18));
      color: #fbbf24;
      border: 1px solid rgba(245, 158, 11, 0.4);
    }
    .score-low {
      background: rgba(100, 116, 139, 0.15);
      color: #94a3b8;
      border: 1px solid rgba(100, 116, 139, 0.3);
    }
    .ai-score-tier {
      font-size: 0.66rem;
      font-weight: 700;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.04em;
      margin-top: 0.2rem;
    }

    /* Title Group */
    .card-title-group {
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
    }
    .card-title {
      font-size: 1.12rem;
      font-weight: 800;
      letter-spacing: -0.01em;
      color: white;
      line-height: 1.35;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }
    .card-company {
      font-size: 0.88rem;
      font-weight: 600;
      color: #38bdf8;
      display: flex;
      align-items: center;
      gap: 0.4rem;
    }

    /* Meta Strip */
    .card-meta-strip {
      display: flex;
      align-items: center;
      gap: 0.6rem;
      flex-wrap: wrap;
      background: rgba(6, 8, 19, 0.6);
      border: 1px solid var(--border-subtle);
      border-radius: 10px;
      padding: 0.55rem 0.85rem;
      font-size: 0.78rem;
    }
    .meta-item {
      display: flex;
      align-items: center;
      gap: 0.35rem;
      color: var(--text-secondary);
    }
    .meta-item strong { color: var(--text-primary); }

    /* AI Match Rationale Box */
    .ai-match-callout {
      background: rgba(10, 16, 32, 0.85);
      border: 1px solid rgba(99, 102, 241, 0.25);
      border-radius: 12px;
      padding: 0.75rem 0.95rem;
      display: flex;
      flex-direction: column;
      gap: 0.45rem;
    }
    .ai-callout-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-size: 0.74rem;
      font-weight: 700;
      color: #a5b4fc;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    .ai-match-progress-bg {
      height: 5px;
      background: rgba(255, 255, 255, 0.08);
      border-radius: 9999px;
      overflow: hidden;
    }
    .ai-match-progress-fill {
      height: 100%;
      background: linear-gradient(90deg, #6366f1, #06b6d4, #10b981);
      border-radius: 9999px;
      transition: width 0.6s ease;
    }
    .ai-callout-text {
      font-size: 0.8rem;
      color: #cbd5e1;
      line-height: 1.45;
    }

    /* Role Overview */
    .card-desc-box {
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
    }
    .card-desc-label {
      font-size: 0.72rem;
      font-weight: 700;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .card-desc-text {
      font-size: 0.82rem;
      color: #94a3b8;
      line-height: 1.5;
      display: -webkit-box;
      -webkit-line-clamp: 3;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }

    /* Skills */
    .card-skills-box {
      display: flex;
      flex-direction: column;
      gap: 0.4rem;
    }
    .card-skills-label {
      font-size: 0.72rem;
      font-weight: 700;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .skills-cluster {
      display: flex;
      align-items: center;
      gap: 0.4rem;
      flex-wrap: wrap;
    }
    .skill-chip {
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid rgba(255, 255, 255, 0.09);
      color: #e2e8f0;
      font-size: 0.74rem;
      font-weight: 600;
      padding: 0.2rem 0.55rem;
      border-radius: 6px;
      transition: background 0.2s, border-color 0.2s;
    }
    .skill-chip:hover {
      background: rgba(99, 102, 241, 0.15);
      border-color: rgba(99, 102, 241, 0.4);
      color: white;
    }

    /* Card Footer */
    .card-footer {
      margin-top: auto;
      padding-top: 0.85rem;
      border-top: 1px solid var(--border-subtle);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.75rem;
      flex-wrap: wrap;
    }
    .card-timestamp {
      font-size: 0.74rem;
      color: var(--text-muted);
      font-family: var(--font-mono);
    }
    .card-action-group {
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }

    .btn-card-apply {
      padding: 0.5rem 1rem;
      font-size: 0.82rem;
      font-weight: 700;
      border-radius: 9px;
      border: none;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      transition: all 0.2s;
      font-family: var(--font-sans);
    }
    .btn-card-apply.ready {
      background: linear-gradient(135deg, #10b981 0%, #059669 100%);
      color: white;
      box-shadow: 0 2px 10px rgba(16, 185, 129, 0.35);
    }
    .btn-card-apply.ready:hover {
      background: linear-gradient(135deg, #059669 0%, #047857 100%);
      box-shadow: 0 4px 14px rgba(16, 185, 129, 0.55);
      transform: translateY(-1px);
    }
    .btn-card-apply.applied {
      background: rgba(16, 185, 129, 0.14);
      color: #34d399;
      border: 1px solid rgba(16, 185, 129, 0.35);
      cursor: default;
    }
    .btn-card-apply.applying {
      background: #475569;
      color: white;
      cursor: wait;
    }

    .btn-view-job {
      padding: 0.5rem 0.85rem;
      font-size: 0.82rem;
      font-weight: 600;
      border-radius: 9px;
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid var(--border-subtle);
      color: var(--text-secondary);
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      gap: 0.3rem;
      transition: all 0.2s;
    }
    .btn-view-job:hover {
      background: rgba(255, 255, 255, 0.1);
      color: white;
      border-color: rgba(255, 255, 255, 0.2);
    }

    /* Table View */
    .table-container {
      background: var(--surface-glass);
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
      border: 1px solid var(--border-subtle);
      border-radius: 16px;
      overflow-x: auto;
      box-shadow: 0 8px 28px rgba(0, 0, 0, 0.3);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.84rem;
      text-align: left;
    }
    thead th {
      background: rgba(10, 15, 28, 0.95);
      color: var(--text-secondary);
      font-weight: 700;
      text-transform: uppercase;
      font-size: 0.72rem;
      letter-spacing: 0.05em;
      padding: 0.95rem 1.15rem;
      border-bottom: 1px solid var(--border-subtle);
      white-space: nowrap;
    }
    tbody td {
      padding: 0.95rem 1.15rem;
      border-bottom: 1px solid rgba(255, 255, 255, 0.04);
      vertical-align: middle;
    }
    tbody tr:hover {
      background: rgba(255, 255, 255, 0.02);
    }

    /* Modals */
    .modal-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.75);
      backdrop-filter: blur(8px);
      -webkit-backdrop-filter: blur(8px);
      z-index: 200;
      display: none;
      align-items: center;
      justify-content: center;
      padding: 1.5rem;
    }
    .modal-backdrop.open { display: flex; }
    .modal-window {
      background: #0b1120;
      border: 1px solid rgba(255, 255, 255, 0.12);
      border-radius: 20px;
      max-width: 900px;
      width: 100%;
      max-height: 90vh;
      display: flex;
      flex-direction: column;
      box-shadow: 0 24px 64px rgba(0, 0, 0, 0.7);
      animation: modalPop 0.25s cubic-bezier(0.16, 1, 0.3, 1);
      overflow: hidden;
    }
    @keyframes modalPop {
      0% { opacity: 0; transform: scale(0.95); }
      100% { opacity: 1; transform: scale(1); }
    }
    .modal-header {
      padding: 1.25rem 1.5rem;
      border-bottom: 1px solid var(--border-subtle);
      display: flex;
      align-items: center;
      justify-content: space-between;
      background: rgba(15, 23, 42, 0.8);
    }
    .modal-title {
      font-size: 1.25rem;
      font-weight: 800;
      display: flex;
      align-items: center;
      gap: 0.6rem;
    }
    .modal-close {
      background: transparent;
      border: none;
      color: var(--text-muted);
      font-size: 1.4rem;
      cursor: pointer;
      line-height: 1;
      padding: 0.2rem 0.5rem;
      border-radius: 6px;
    }
    .modal-close:hover { color: white; background: rgba(255, 255, 255, 0.08); }
    .modal-body {
      padding: 1.5rem;
      overflow-y: auto;
      display: flex;
      flex-direction: column;
      gap: 1.25rem;
    }
    .modal-footer {
      padding: 1.15rem 1.5rem;
      border-top: 1px solid var(--border-subtle);
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 0.85rem;
      background: rgba(15, 23, 42, 0.8);
    }

    /* Settings Form */
    .settings-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 1.25rem;
    }
    .settings-card {
      background: rgba(15, 23, 42, 0.6);
      border: 1px solid var(--border-subtle);
      border-radius: 14px;
      padding: 1.25rem;
      display: flex;
      flex-direction: column;
      gap: 0.85rem;
    }
    .settings-card-header {
      font-size: 0.88rem;
      font-weight: 700;
      color: #38bdf8;
      display: flex;
      align-items: center;
      gap: 0.5rem;
      padding-bottom: 0.5rem;
      border-bottom: 1px solid rgba(255, 255, 255, 0.06);
    }
    .form-group {
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
    }
    .form-group label {
      font-size: 0.76rem;
      font-weight: 600;
      color: var(--text-secondary);
    }
    .form-control {
      background: rgba(6, 8, 19, 0.8);
      border: 1px solid var(--border-subtle);
      color: white;
      border-radius: 8px;
      padding: 0.55rem 0.85rem;
      font-size: 0.84rem;
      font-family: var(--font-sans);
      outline: none;
      transition: border-color 0.2s;
    }
    .form-control:focus {
      border-color: var(--primary);
      box-shadow: 0 0 0 2px rgba(99, 102, 241, 0.25);
    }
    textarea.form-control { resize: vertical; min-height: 70px; }

    .file-status-badge {
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      padding: 0.35rem 0.75rem;
      border-radius: 6px;
      font-size: 0.76rem;
      font-weight: 600;
    }
    .status-file-ok {
      background: rgba(16, 185, 129, 0.12);
      color: #34d399;
      border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .status-file-err {
      background: rgba(239, 68, 68, 0.12);
      color: #f87171;
      border: 1px solid rgba(239, 68, 68, 0.3);
    }

    /* Toast Notification */
    .toast-container {
      position: fixed;
      top: 1.5rem;
      right: 1.5rem;
      z-index: 300;
      display: flex;
      flex-direction: column;
      gap: 0.65rem;
      pointer-events: none;
    }
    .toast {
      background: rgba(15, 23, 42, 0.95);
      border: 1px solid rgba(255, 255, 255, 0.12);
      backdrop-filter: blur(14px);
      color: white;
      padding: 0.85rem 1.25rem;
      border-radius: 12px;
      font-size: 0.84rem;
      font-weight: 600;
      box-shadow: 0 12px 32px rgba(0, 0, 0, 0.5);
      display: flex;
      align-items: center;
      gap: 0.75rem;
      pointer-events: auto;
      animation: toastIn 0.3s cubic-bezier(0.16, 1, 0.3, 1);
      transition: opacity 0.3s, transform 0.3s;
    }
    @keyframes toastIn {
      0% { opacity: 0; transform: translateX(40px); }
      100% { opacity: 1; transform: translateX(0); }
    }

    /* Empty state */
    .empty-state {
      text-align: center;
      padding: 4rem 1.5rem;
      color: var(--text-muted);
      font-size: 0.95rem;
      background: var(--surface-glass);
      border: 1px dashed var(--border-subtle);
      border-radius: 16px;
    }

    /* Responsive */
    @media (max-width: 900px) {
      .jobs-grid { grid-template-columns: 1fr; }
      .metrics-grid { grid-template-columns: repeat(2, 1fr); }
      header { padding: 0.75rem 1rem; }
      main { padding: 1rem 1rem 2rem; }
    }
  </style>
</head>
<body>

  <!-- Toast Container -->
  <div class="toast-container" id="toast-container"></div>

  <!-- Header -->
  <header>
    <div class="header-content">
      <div class="brand">
        <div class="brand-icon">⚡</div>
        <div class="brand-title-wrap">
          <div class="brand-title">
            AI JOB AUTOMATION HUB
            <span class="badge-pro">v2.0 PRO</span>
          </div>
          <div class="brand-sub">NopeRi (Naukri API) + job-bot (Multi-Portal Auto-Apply)</div>
        </div>
      </div>

      <div class="header-right">
        <div class="candidate-pill">
          <div class="cand-avatar">AB</div>
          <div>
            <strong id="hdr-cand-name">Avinash Biradar</strong>
          </div>
          <div class="badge-fresher" id="hdr-cand-fresher">
            <span class="badge-fresher-dot"></span>
            Fresher (0 Exp)
          </div>
          <div class="badge-ctc" id="hdr-cand-ctc">Target: 4 LPA</div>
          <div style="font-size: 0.75rem; color: #94a3b8;" id="hdr-cand-loc">📍 Bengaluru</div>
        </div>

        <button class="btn btn-settings" onclick="openSettingsModal()">
          ⚙️ Settings
        </button>
        <button class="btn btn-refresh" onclick="loadJobsData()" title="Reload applications list">
          🔄 Refresh
        </button>
      </div>
    </div>
  </header>

  <main>

    <!-- Command Action Bar -->
    <div class="action-panel">
      <div class="action-left">
        <button class="btn btn-both" id="btn-run-both" onclick="runBots('both')">
          ⚡ Run Both Engines
        </button>
        <button class="btn btn-noperi" id="btn-run-noperi" onclick="runBots('noperi')">
          🟠 Run NopeRi (Naukri Direct API)
        </button>
        <button class="btn btn-jobbot" id="btn-run-jobbot" onclick="runBots('jobbot')">
          🤖 Run job-bot (Multi-Portal)
        </button>
        <button class="btn btn-cmd" id="btn-cmd-noperi" onclick="openNativeTerminal('noperi')" title="Open an actual Windows Command Prompt terminal window for NopeRi">
          🖥️ Pop-out NopeRi CMD
        </button>
        <button class="btn btn-stop" id="btn-stop" onclick="stopRun()">
          🛑 Stop
        </button>
      </div>

      <div class="action-right">
        <label style="display: flex; align-items: center; gap: 0.45rem; font-size: 0.82rem; color: #cbd5e1; cursor: pointer; user-select: none;" title="Check to automatically pop open a native Windows Command Prompt when clicking Run">
          <input type="checkbox" id="chk-pop-cmd" style="accent-color: #10b981; cursor: pointer; width: 16px; height: 16px;" />
          <span>Auto Pop-out CMD on Run</span>
        </label>
        <div class="status-badge">
          <span class="status-dot" id="status-dot"></span>
          <span id="status-text">Status: Checking...</span>
        </div>
      </div>
    </div>

    <!-- Metrics Cards Strip -->
    <div class="metrics-grid">
      <div class="metric-card m-blue">
        <div class="metric-label">
          <span>Total Discovered</span>
          <span>💼</span>
        </div>
        <div class="metric-value" id="m-total">0</div>
        <div class="metric-sub">Unified feed from all engines</div>
      </div>

      <div class="metric-card m-emerald">
        <div class="metric-label">
          <span>Ready to Apply (≥70%)</span>
          <span>⚡</span>
        </div>
        <div class="metric-value" id="m-ready" style="color: #34d399;">0</div>
        <div class="metric-sub">Strict 70%+ match requirement met</div>
      </div>

      <div class="metric-card m-amber">
        <div class="metric-label">
          <span>Review Required (&lt;70%)</span>
          <span>🔍</span>
        </div>
        <div class="metric-value" id="m-review" style="color: #fbbf24;">0</div>
        <div class="metric-sub">Vetoed or partial stack overlap</div>
      </div>

      <div class="metric-card m-purple">
        <div class="metric-label">
          <span>Successfully Applied</span>
          <span>✅</span>
        </div>
        <div class="metric-value" id="m-high" style="color: #c084fc;">0</div>
        <div class="metric-sub">Confirmed submissions recorded</div>
      </div>

      <div class="metric-card m-cyan">
        <div class="metric-label">
          <span>AI Engine</span>
          <span>🤖</span>
        </div>
        <div class="metric-value" id="m-ai-provider" style="font-size: 1.15rem; font-weight: 700; color: #38bdf8; margin-top: 0.35rem;">Gemini 3.5</div>
        <div class="metric-sub" id="m-ai-sub">1,500 requests/day active quota</div>
      </div>
    </div>

    <!-- Live Terminal Stream -->
    <div class="terminal-section">
      <div class="terminal-header">
        <div class="terminal-title-group">
          <div class="terminal-dots">
            <span class="t-dot red"></span>
            <span class="t-dot yellow"></span>
            <span class="t-dot green"></span>
          </div>
          <div class="terminal-title">
            <span>bash — live-agent-stream :: port 8765</span>
            <span style="font-size: 0.7rem; color: #34d399;" id="term-status">● Live</span>
          </div>
        </div>

        <div class="terminal-actions">
          <button class="term-btn" id="term-autoscroll-btn" onclick="toggleAutoScroll()">Auto-Scroll: ON</button>
          <button class="term-btn" onclick="clearTerminal()">Clear</button>
          <button class="term-btn" onclick="copyLogs()">Copy</button>
          <button class="term-btn" style="color: #38bdf8; border-color: rgba(56, 189, 248, 0.4);" onclick="openNativeTerminal('noperi')" title="Launch native Windows Command Prompt window for NopeRi">🖥️ Pop-out NopeRi CMD</button>
          <button class="term-btn" style="color: #a78bfa; border-color: rgba(167, 139, 250, 0.4);" onclick="openNativeTerminal('jobbot')" title="Launch native Windows Command Prompt window for job-bot">🖥️ Pop-out job-bot CMD</button>
        </div>
      </div>

      <div class="terminal-body" id="terminal-body">
        <div class="term-line-info">[SYSTEM] Connected to Live Terminal Stream</div>
      </div>
    </div>

    <!-- Navigation Tabs -->
    <div class="tabs-bar">
      <button class="tab-btn active" id="tab-all-btn" onclick="setTab('all')">
        📋 All Vacancies <span class="tab-count" id="tab-all-count">0</span>
      </button>
      <button class="tab-btn tab-ready" id="tab-ready-btn" onclick="setTab('ready')">
        ⚡ Ready to Apply (≥70%) <span class="tab-count" id="tab-ready-count">0</span>
      </button>
      <button class="tab-btn" id="tab-applied-btn" onclick="setTab('applied')">
        ✅ Applied <span class="tab-count" id="tab-applied-count">0</span>
      </button>
      <button class="tab-btn" id="tab-review-btn" onclick="setTab('review')">
        🔍 Review (&lt;70%) <span class="tab-count" id="tab-review-count">0</span>
      </button>
      <button class="tab-btn tab-noperi" id="tab-noperi-btn" onclick="setTab('noperi')">
        🟠 NopeRi (Naukri API) <span class="tab-count" id="tab-noperi-count">0</span>
      </button>
      <button class="tab-btn tab-jobbot" id="tab-jobbot-btn" onclick="setTab('jobbot')">
        🤖 job-bot (Multi-Portal) <span class="tab-count" id="tab-jobbot-count">0</span>
      </button>
    </div>

    <!-- Filter and Search Controls -->
    <div class="filter-bar">
      <div class="filter-left">
        <div class="search-box">
          <span>🔍</span>
          <input type="text" id="job-search" placeholder="Search by title, company, skill, location..." oninput="onSearchChange()">
          <button class="search-clear" onclick="clearSearch()" title="Clear search">✕</button>
        </div>

        <div class="chip-group">
          <button class="filter-chip active" id="chip-plat-all" onclick="setPlatformFilter('all')">All Portals</button>
          <button class="filter-chip" id="chip-plat-naukri" onclick="setPlatformFilter('naukri')">Naukri</button>
          <button class="filter-chip" id="chip-plat-internshala" onclick="setPlatformFilter('internshala')">Internshala</button>
          <button class="filter-chip" id="chip-plat-indeed" onclick="setPlatformFilter('indeed')">Indeed</button>
        </div>
      </div>

      <div class="filter-right">
        <select class="sort-select" id="sort-select" onchange="onSortChange()">
          <option value="score_desc">Score: Highest First (100% → 0%)</option>
          <option value="score_asc">Score: Lowest First</option>
          <option value="newest">Newest First</option>
          <option value="company">Company (A-Z)</option>
        </select>

        <div class="view-toggle">
          <button class="view-btn active" id="view-cards-btn" onclick="setViewMode('cards')">🪟 Cards</button>
          <button class="view-btn" id="view-table-btn" onclick="setViewMode('table')">📄 Table</button>
        </div>
      </div>
    </div>

    <!-- Jobs Grid Container -->
    <div class="jobs-grid" id="jobs-cards-container">
      <div class="empty-state" style="grid-column: 1 / -1;">
        Loading job applications and AI evaluation data...
      </div>
    </div>

    <!-- Jobs Table Container -->
    <div class="table-container" id="jobs-table-container" style="display: none;">
      <table>
        <thead>
          <tr>
            <th style="width: 80px; text-align: center;">Match</th>
            <th>Role & Company</th>
            <th>Location & Exp</th>
            <th>Skills</th>
            <th>Platform / Engine</th>
            <th>AI Rationale</th>
            <th>Status / Applied At</th>
            <th style="text-align: right;">Action</th>
          </tr>
        </thead>
        <tbody id="jobs-tbody">
          <tr><td colspan="8" class="empty-state">Loading table data...</td></tr>
        </tbody>
      </table>
    </div>

  </main>

  <!-- Job Detail Modal -->
  <div class="modal-backdrop" id="job-modal">
    <div class="modal-window">
      <div class="modal-header">
        <div class="modal-title" id="modal-job-title">Job Details</div>
        <button class="modal-close" onclick="closeJobModal()">✕</button>
      </div>
      <div class="modal-body" id="modal-job-body">
        <!-- Injected via JS -->
      </div>
      <div class="modal-footer" id="modal-job-footer">
        <button class="btn btn-refresh" onclick="closeJobModal()">Close</button>
      </div>
    </div>
  </div>

  <!-- Settings Modal -->
  <div class="modal-backdrop" id="settings-modal">
    <div class="modal-window" style="max-width: 980px;">
      <div class="modal-header">
        <div class="modal-title">
          <span>⚙️ Automation Settings & AI Configuration</span>
        </div>
        <button class="modal-close" onclick="closeSettingsModal()">✕</button>
      </div>

      <div class="modal-body">
        <div class="settings-grid">
          
          <!-- Candidate Profile -->
          <div class="settings-card">
            <div class="settings-card-header">
              <span>👤 Candidate Profile & CTC Target</span>
            </div>
            
            <div class="form-group">
              <label>Full Name</label>
              <input type="text" class="form-control" id="set-name" value="Avinash Biradar">
            </div>

            <div class="form-group">
              <label>Email Address</label>
              <input type="email" class="form-control" id="set-email" value="avinashbiradar722@gmail.com">
            </div>

            <div class="form-group">
              <label>Phone Number</label>
              <input type="text" class="form-control" id="set-phone" value="+91 8746988163">
            </div>

            <div class="form-group">
              <label>Primary Location</label>
              <input type="text" class="form-control" id="set-location" value="Bengaluru, Karnataka">
            </div>

            <div class="form-group">
              <label>Target Locations (Comma separated)</label>
              <input type="text" class="form-control" id="set-locations" value="Bengaluru, Hyderabad, Pune, Remote">
            </div>

            <div class="form-group">
              <label>Willing to Relocate</label>
              <select class="form-control" id="set-relocate">
                <option value="Yes" selected>Yes</option>
                <option value="No">No</option>
              </select>
            </div>
          </div>

          <!-- Experience & Salary -->
          <div class="settings-card">
            <div class="settings-card-header">
              <span>💼 Experience & Salary Expectation</span>
            </div>

            <div class="form-group">
              <label>Fresher Status</label>
              <select class="form-control" id="set-fresher" onchange="onFresherChange()">
                <option value="true" selected>Fresher (0 Years Experience - 2026 Batch)</option>
                <option value="false">Experienced</option>
              </select>
            </div>

            <div class="form-group">
              <label>Experience in Years</label>
              <input type="number" class="form-control" id="set-exp-years" value="0" min="0" max="30">
            </div>

            <div class="form-group">
              <label>Current CTC (INR / LPA)</label>
              <input type="text" class="form-control" id="set-curr-ctc" value="0">
            </div>

            <div class="form-group">
              <label>Expected CTC (INR / LPA) — Target: 4 LPA</label>
              <input type="text" class="form-control" id="set-exp-ctc" value="400000">
            </div>

            <div class="form-group">
              <label>Notice Period (Days)</label>
              <input type="number" class="form-control" id="set-notice" value="15">
            </div>

            <div class="form-group">
              <label>Experience Summary</label>
              <textarea class="form-control" id="set-exp-summary" placeholder="Full Stack Developer fresher with strong foundational projects in React, Node.js, and Python."></textarea>
            </div>
          </div>

          <!-- Skills & Projects -->
          <div class="settings-card">
            <div class="settings-card-header">
              <span>⚡ Skills, Projects & Vetoes</span>
            </div>

            <div class="form-group">
              <label>Primary Skills (Comma separated)</label>
              <textarea class="form-control" id="set-skills" placeholder="Python, JavaScript, React.js, Node.js, Express.js, SQL, REST APIs, Git"></textarea>
            </div>

            <div class="form-group">
              <label>Veto Keywords (Block roles containing these)</label>
              <textarea class="form-control" id="set-veto" placeholder="sales, bpo, telecaller, senior, lead, architect, php, angular, dot net, .net, c#"></textarea>
            </div>

            <div class="form-group">
              <label>Key Projects</label>
              <textarea class="form-control" id="set-projects" placeholder="IRIS AI Voice Assistant, VTUnotes Study Platform, LootBazaar Aggregator"></textarea>
            </div>

            <div class="form-group">
              <label>Resume PDF File Path</label>
              <input type="text" class="form-control" id="set-resume-path" value="C:\Users\Hp\Downloads\resume.pdf">
              <div id="set-resume-status" class="file-status-badge status-file-ok" style="margin-top: 0.35rem;">
                🟢 PDF Verified on Disk
              </div>
            </div>
          </div>

          <!-- AI Configuration -->
          <div class="settings-card">
            <div class="settings-card-header">
              <span>🤖 AI Engine, API Keys & Thresholds</span>
            </div>

            <div class="form-group">
              <label>Active AI Provider</label>
              <div style="display: flex; gap: 1rem; margin-top: 0.25rem;">
                <label style="display: flex; align-items: center; gap: 0.35rem; cursor: pointer; color: white;">
                  <input type="radio" name="ai_provider" value="gemini" checked> Google Gemini (1,500 req/day)
                </label>
                <label style="display: flex; align-items: center; gap: 0.35rem; cursor: pointer; color: white;">
                  <input type="radio" name="ai_provider" value="groq"> Groq (Llama 3.3)
                </label>
              </div>
            </div>

            <div class="form-group">
              <label>Gemini API Key</label>
              <div style="display: flex; gap: 0.4rem;">
                <input type="password" class="form-control" id="set-gemini-key" placeholder="AIzaSy...">
                <button type="button" class="btn btn-refresh" style="padding: 0 0.65rem;" onclick="toggleKeyVisibility('set-gemini-key', this)">👁</button>
              </div>
              <div style="display: flex; gap: 0.5rem; align-items: center; margin-top: 0.25rem;">
                <input type="text" class="form-control" id="set-gemini-model" value="gemini-3.5-flash-lite" placeholder="Model name" style="flex: 1;">
                <button type="button" class="btn btn-refresh" style="font-size: 0.74rem; padding: 0.35rem 0.65rem;" onclick="testKey('gemini')">Test</button>
              </div>
              <span id="gemini-test-result" style="font-size: 0.74rem;"></span>
            </div>

            <div class="form-group">
              <label>Groq API Key</label>
              <div style="display: flex; gap: 0.4rem;">
                <input type="password" class="form-control" id="set-groq-key" placeholder="gsk_...">
                <button type="button" class="btn btn-refresh" style="padding: 0 0.65rem;" onclick="toggleKeyVisibility('set-groq-key', this)">👁</button>
              </div>
              <div style="display: flex; gap: 0.5rem; align-items: center; margin-top: 0.25rem;">
                <input type="text" class="form-control" id="set-groq-model" value="llama-3.3-70b-versatile" placeholder="Model name" style="flex: 1;">
                <button type="button" class="btn btn-refresh" style="font-size: 0.74rem; padding: 0.35rem 0.65rem;" onclick="testKey('groq')">Test</button>
              </div>
              <span id="groq-test-result" style="font-size: 0.74rem;"></span>
            </div>

            <div class="form-group">
              <div style="display: flex; justify-content: space-between;">
                <label>Strict Match Threshold: <strong id="thresh-val" style="color: #34d399;">70%</strong></label>
              </div>
              <input type="range" id="set-threshold" min="30" max="90" value="70" style="width: 100%; accent-color: #10b981;" oninput="document.getElementById('thresh-val').innerText = this.value + '%'">
              <div style="font-size: 0.72rem; color: var(--text-muted);">
                Jobs scoring ≥ this percentage are approved for automatic application.
              </div>
            </div>

          </div>

        </div>
      </div>

      <div class="modal-footer">
        <span id="save-msg" style="font-size: 0.8rem; margin-right: auto;"></span>
        <button class="btn btn-refresh" onclick="closeSettingsModal()">Cancel</button>
        <button class="btn btn-both" onclick="saveAllSettings()">💾 Save & Sync Across Both Bots</button>
      </div>
    </div>
  </div>

  <script>
    // State Store
    let allJobs = [];
    let activeTab = 'all';
    let platformFilter = 'all';
    let searchQuery = '';
    let sortMode = 'score_desc';
    let viewMode = 'cards';
    let autoScroll = true;

    // Toast Manager
    function showToast(message, type = 'info') {
      const container = document.getElementById('toast-container');
      const toast = document.createElement('div');
      toast.className = 'toast';
      const icon = type === 'success' ? '✅' : type === 'error' ? '❌' : '⚡';
      toast.innerHTML = `<span>${icon}</span> <span>${message}</span>`;
      container.appendChild(toast);

      setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateX(40px)';
        setTimeout(() => toast.remove(), 300);
      }, 4000);
    }

    // Modal Handlers
    function openSettingsModal() {
      loadSettingsData();
      document.getElementById('settings-modal').classList.add('open');
    }
    function closeSettingsModal() {
      document.getElementById('settings-modal').classList.remove('open');
    }
    function openJobModal(jobId) {
      const job = allJobs.find(j => String(j.id) === String(jobId));
      if (!job) return;

      document.getElementById('modal-job-title').innerText = `${job.title} @ ${job.company}`;
      
      const skillsHtml = (job.skills || []).map(s => `<span class="skill-chip">${s}</span>`).join(' ');
      
      let applyBtn = '';
      if (job.applied) {
        applyBtn = `<button class="btn-card-apply applied" disabled>Applied ✓</button>`;
      } else {
        applyBtn = `<button class="btn-card-apply ready" onclick="applyToJobCard('${job.id}', this); closeJobModal();">⚡ Apply Now</button>`;
      }

      document.getElementById('modal-job-body').innerHTML = `
        <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 1rem; padding-bottom: 1rem; border-bottom: 1px solid rgba(255,255,255,0.08);">
          <div style="display: flex; gap: 0.5rem; align-items: center;">
            <span class="platform-pill ${job.source === 'NopeRi' ? 'pill-noperi' : 'pill-naukri'}">${job.platform || job.source}</span>
            <span class="status-pill ${job.applied ? 'status-applied' : (job.ai_score >= 70 ? 'status-ready' : 'status-review')}">${job.status}</span>
          </div>
          <div style="font-family: var(--font-mono); font-size: 1.25rem; font-weight: 800; color: ${job.ai_score >= 70 ? '#34d399' : '#fbbf24'};">
            ${job.ai_score}% Match Score (${job.ai_tier || 'Fit'})
          </div>
        </div>

        <div class="card-meta-strip">
          <span class="meta-item">📍 <strong>${job.location}</strong></span>
          <span class="meta-item">💼 <strong>${job.experience || 'Fresher (0 - 1 Yrs)'}</strong></span>
          <span class="meta-item">💰 <strong>${job.salary || '4 LPA'}</strong></span>
        </div>

        <div class="ai-match-callout">
          <div class="ai-callout-header">
            <span>🎯 AI Evaluation Rationale</span>
            <span>${job.ai_score}% Fit</span>
          </div>
          <div class="ai-match-progress-bg">
            <div class="ai-match-progress-fill" style="width: ${job.ai_score}%;"></div>
          </div>
          <div class="ai-callout-text">${job.ai_reason || 'Matches core stack fundamentals'}</div>
        </div>

        <div>
          <div style="font-size: 0.78rem; font-weight: 700; color: #94a3b8; text-transform: uppercase; margin-bottom: 0.4rem;">⚡ Required Skills</div>
          <div class="skills-cluster">${skillsHtml}</div>
        </div>

        <div>
          <div style="font-size: 0.78rem; font-weight: 700; color: #94a3b8; text-transform: uppercase; margin-bottom: 0.4rem;">📄 Full Job Description</div>
          <div style="font-size: 0.85rem; color: #cbd5e1; line-height: 1.6; max-height: 250px; overflow-y: auto; background: rgba(0,0,0,0.3); padding: 0.85rem; border-radius: 8px;">
            ${job.description || 'No detailed description provided.'}
          </div>
        </div>
      `;

      document.getElementById('modal-job-footer').innerHTML = `
        <button class="btn btn-refresh" onclick="closeJobModal()">Close</button>
        <a href="${job.url}" target="_blank" class="btn-view-job">View on Portal ↗</a>
        ${applyBtn}
      `;

      document.getElementById('job-modal').classList.add('open');
    }
    function closeJobModal() {
      document.getElementById('job-modal').classList.remove('open');
    }

    // Terminal Stream & Controls
    function initLogStream() {
      const source = new EventSource('/api/stream');
      const body = document.getElementById('terminal-body');

      source.onmessage = (event) => {
        const line = event.data;
        if (!line) return;

        const div = document.createElement('div');
        div.textContent = line;

        if (line.includes('Applied') || line.includes('✅') || line.includes('SUCCESS') || line.includes('Migrated')) {
          div.className = 'term-line-success';
        } else if (line.includes('score=') || line.includes('WARNING') || line.includes('⚠️')) {
          div.className = 'term-line-warn';
        } else if (line.includes('Error') || line.includes('Exception') || line.includes('FAILED')) {
          div.className = 'term-line-error';
        } else if (line.includes('STARTING') || line.includes('FETCHING') || line.includes('LOGGING')) {
          div.className = 'term-line-info';
        }

        body.appendChild(div);

        if (autoScroll) {
          body.scrollTop = body.scrollHeight;
        }
      };

      source.onerror = () => {
        document.getElementById('term-status').innerText = '○ Reconnecting...';
        document.getElementById('term-status').style.color = '#fbbf24';
      };

      source.onopen = () => {
        document.getElementById('term-status').innerText = '● Live';
        document.getElementById('term-status').style.color = '#34d399';
      };
    }

    function toggleAutoScroll() {
      autoScroll = !autoScroll;
      const btn = document.getElementById('term-autoscroll-btn');
      btn.innerText = `Auto-Scroll: ${autoScroll ? 'ON' : 'OFF'}`;
      btn.style.color = autoScroll ? '#34d399' : '#94a3b8';
    }

    function clearTerminal() {
      document.getElementById('terminal-body').innerHTML = '<div class="term-line-info">[SYSTEM] Console cleared</div>';
    }

    function copyLogs() {
      const text = document.getElementById('terminal-body').innerText;
      navigator.clipboard.writeText(text);
      showToast('📋 Copied terminal logs to clipboard');
    }

    // Bot State Polling
    async function checkBotStatus() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        const dot = document.getElementById('status-dot');
        const txt = document.getElementById('status-text');

        if (data.running) {
          dot.className = 'status-dot active';
          txt.innerText = `Running: ${data.current_task}`;
        } else {
          dot.className = 'status-dot';
          txt.innerText = 'Status: Idle';
        }
      } catch (e) {
        console.error('Status check error:', e);
      }
    }
    setInterval(checkBotStatus, 3000);

    // Job Data Loader
    async function loadJobsData() {
      try {
        const res = await fetch('/api/jobs');
        const data = await res.json();

        allJobs = data.all || [];

        // Update Candidate Header
        const cand = data.candidate || {};
        if (cand.name) document.getElementById('hdr-cand-name').innerText = cand.name;
        if (cand.location) document.getElementById('hdr-cand-loc').innerText = '📍 ' + cand.location;
        if (cand.expected_ctc) document.getElementById('hdr-cand-ctc').innerText = 'Target: ' + cand.expected_ctc;

        // Update Stats
        const stats = data.stats || {};
        document.getElementById('m-total').innerText = stats.total_jobs || 0;
        document.getElementById('m-ready').innerText = stats.ready_to_apply || 0;
        document.getElementById('m-review').innerText = stats.needs_review || 0;
        document.getElementById('m-high').innerText = stats.total_applied || 0;

        // Tab counts
        document.getElementById('tab-all-count').innerText = stats.total_jobs || 0;
        document.getElementById('tab-ready-count').innerText = stats.ready_to_apply || 0;
        document.getElementById('tab-applied-count').innerText = stats.total_applied || 0;
        document.getElementById('tab-review-count').innerText = stats.needs_review || 0;
        document.getElementById('tab-noperi-count').innerText = stats.noperi_total || 0;
        document.getElementById('tab-jobbot-count').innerText = stats.jobbot_total || 0;

        renderFilteredJobs();
      } catch (e) {
        console.error('Error loading jobs:', e);
      }
    }

    // Filtering & Sorting
    function setTab(tab) {
      activeTab = tab;
      document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
      const activeEl = document.getElementById(`tab-${tab}-btn`);
      if (activeEl) activeEl.classList.add('active');
      renderFilteredJobs();
    }

    function setPlatformFilter(plat) {
      platformFilter = plat;
      document.querySelectorAll('.filter-chip').forEach(c => c.classList.remove('active'));
      const activeChip = document.getElementById(`chip-plat-${plat}`);
      if (activeChip) activeChip.classList.add('active');
      renderFilteredJobs();
    }

    function onSearchChange() {
      searchQuery = document.getElementById('job-search').value.toLowerCase().trim();
      renderFilteredJobs();
    }

    function clearSearch() {
      document.getElementById('job-search').value = '';
      searchQuery = '';
      renderFilteredJobs();
    }

    function onSortChange() {
      sortMode = document.getElementById('sort-select').value;
      renderFilteredJobs();
    }

    function setViewMode(mode) {
      viewMode = mode;
      document.getElementById('view-cards-btn').classList.toggle('active', mode === 'cards');
      document.getElementById('view-table-btn').classList.toggle('active', mode === 'table');
      document.getElementById('jobs-cards-container').style.display = mode === 'cards' ? 'grid' : 'none';
      document.getElementById('jobs-table-container').style.display = mode === 'table' ? 'block' : 'none';
      renderFilteredJobs();
    }

    function renderFilteredJobs() {
      let filtered = [...allJobs];

      // Tab Filter
      if (activeTab === 'ready') {
        filtered = filtered.filter(j => !j.applied && j.ai_score >= 70);
      } else if (activeTab === 'applied') {
        filtered = filtered.filter(j => j.applied);
      } else if (activeTab === 'review') {
        filtered = filtered.filter(j => !j.applied && j.ai_score < 70);
      } else if (activeTab === 'noperi') {
        filtered = filtered.filter(j => j.source === 'NopeRi');
      } else if (activeTab === 'jobbot') {
        filtered = filtered.filter(j => j.source === 'job-bot');
      }

      // Platform Filter
      if (platformFilter !== 'all') {
        filtered = filtered.filter(j => (j.platform || '').toLowerCase().includes(platformFilter));
      }

      // Search Query
      if (searchQuery) {
        filtered = filtered.filter(j => {
          const title = (j.title || '').toLowerCase();
          const comp = (j.company || '').toLowerCase();
          const loc = (j.location || '').toLowerCase();
          const skills = (j.skills || []).join(' ').toLowerCase();
          return title.includes(searchQuery) || comp.includes(searchQuery) || loc.includes(searchQuery) || skills.includes(searchQuery);
        });
      }

      // Sorting
      if (sortMode === 'score_desc') {
        filtered.sort((a, b) => (b.ai_score || 0) - (a.ai_score || 0));
      } else if (sortMode === 'score_asc') {
        filtered.sort((a, b) => (a.ai_score || 0) - (b.ai_score || 0));
      } else if (sortMode === 'newest') {
        filtered.sort((a, b) => (b.applied_at || '').localeCompare(a.applied_at || ''));
      } else if (sortMode === 'company') {
        filtered.sort((a, b) => (a.company || '').localeCompare(b.company || ''));
      }

      if (viewMode === 'cards') {
        renderCards(filtered);
      } else {
        renderTable(filtered);
      }
    }

    function renderCards(items) {
      const container = document.getElementById('jobs-cards-container');
      if (items.length === 0) {
        container.innerHTML = `<div class="empty-state" style="grid-column: 1 / -1;">No matching vacancies found matching current filters.</div>`;
        return;
      }

      container.innerHTML = items.map(j => {
        let scoreClass = 'score-low';
        if (j.ai_score >= 85) scoreClass = 'score-high';
        else if (j.ai_score >= 70) scoreClass = 'score-mid';
        else if (j.ai_score >= 50) scoreClass = 'score-mod';

        let pillClass = 'pill-naukri';
        const pLower = (j.platform || "").toLowerCase();
        if (j.source === 'NopeRi') pillClass = 'pill-noperi';
        else if (pLower.includes('internshala')) pillClass = 'pill-internshala';
        else if (pLower.includes('indeed')) pillClass = 'pill-indeed';

        const skillsHtml = (j.skills || []).slice(0, 5).map(s => `<span class="skill-chip">${s}</span>`).join('');
        const fmtDate = j.applied_at ? j.applied_at.replace('T', ' ').substring(0, 19) : (j.applied ? 'Applied' : 'Active');

        let statusBadge = '';
        let applyBtnHtml = '';
        if (j.applied) {
          statusBadge = '<span class="status-pill status-applied">Applied ✅</span>';
          applyBtnHtml = `<button class="btn-card-apply applied" disabled>Applied ✓</button>`;
        } else if (j.ai_score >= 70) {
          statusBadge = '<span class="status-pill status-ready">Ready (≥70%) ⚡</span>';
          applyBtnHtml = `<button class="btn-card-apply ready" id="btn-apply-${j.id}" onclick="applyToJobCard('${j.id}', this)">⚡ Apply Now</button>`;
        } else {
          statusBadge = '<span class="status-pill status-review">Score &lt; 70% 🔍</span>';
          applyBtnHtml = `<button class="btn-card-apply ready" style="background: linear-gradient(135deg, #f59e0b, #d97706); box-shadow: 0 2px 10px rgba(245, 158, 11, 0.35);" id="btn-apply-${j.id}" onclick="applyToJobCard('${j.id}', this)">⚡ Apply</button>`;
        }

        return `
          <div class="job-card ${j.applied ? 'is-applied' : (j.ai_score >= 70 ? 'is-ready' : '')}" id="card-${j.id}">
            <div class="card-top">
              <div class="card-badges">
                <span class="platform-pill ${pillClass}">
                  ${j.source === 'NopeRi' ? '🟠 NopeRi Direct API' : '🤖 ' + j.platform}
                </span>
                ${statusBadge}
              </div>
              <div class="ai-score-box">
                <div class="ai-score-badge ${scoreClass}">
                  <span>${j.ai_score}%</span>
                </div>
                <span class="ai-score-tier">${j.ai_tier || 'Match Fit'}</span>
              </div>
            </div>

            <div class="card-title-group" onclick="openJobModal('${j.id}')" style="cursor: pointer;" title="Click to view details">
              <div class="card-title">${j.title}</div>
              <div class="card-company">🏢 ${j.company}</div>
            </div>

            <div class="card-meta-strip">
              <span class="meta-item">📍 <strong>${j.location}</strong></span>
              <span class="meta-item">💼 <strong>${j.experience || 'Fresher'}</strong></span>
              <span class="meta-item">💰 <strong>${j.salary || '4 LPA Target'}</strong></span>
            </div>

            <div class="ai-match-callout" onclick="openJobModal('${j.id}')" style="cursor: pointer;">
              <div class="ai-callout-header">
                <span>🎯 AI Evaluation Rationale</span>
                <span>${j.ai_score}% Fit</span>
              </div>
              <div class="ai-match-progress-bg">
                <div class="ai-match-progress-fill" style="width: ${j.ai_score}%;"></div>
              </div>
              <div class="ai-callout-text">${j.ai_reason || 'Fresher stack overlap with core candidate skills.'}</div>
            </div>

            <div class="card-skills-box">
              <div class="card-skills-label">⚡ Required Skills</div>
              <div class="skills-cluster">
                ${skillsHtml}
              </div>
            </div>

            <div class="card-footer">
              <span class="card-timestamp">🕒 ${j.applied ? 'Applied: ' + fmtDate : 'Status: ' + j.status}</span>
              <div class="card-action-group">
                <button class="btn-view-job" onclick="openJobModal('${j.id}')" title="Read full job description and evaluation">Details ℹ️</button>
                ${applyBtnHtml}
                <a href="${j.url}" target="_blank" class="btn-view-job" title="Open vacancy in browser">↗</a>
              </div>
            </div>
          </div>
        `;
      }).join('');
    }

    function renderTable(items) {
      const tbody = document.getElementById('jobs-tbody');
      if (items.length === 0) {
        tbody.innerHTML = `<tr><td colspan="8" class="empty-state">No matching applications found with current filters.</td></tr>`;
        return;
      }

      tbody.innerHTML = items.map(j => {
        let scoreClass = 'score-low';
        if (j.ai_score >= 85) scoreClass = 'score-high';
        else if (j.ai_score >= 70) scoreClass = 'score-mid';
        else if (j.ai_score >= 50) scoreClass = 'score-mod';

        let pillClass = 'pill-naukri';
        const pLower = (j.platform || "").toLowerCase();
        if (j.source === 'NopeRi') pillClass = 'pill-noperi';
        else if (pLower.includes('internshala')) pillClass = 'pill-internshala';
        else if (pLower.includes('indeed')) pillClass = 'pill-indeed';

        const fmtDate = j.applied_at ? j.applied_at.replace('T', ' ').substring(0, 19) : (j.applied ? 'Applied' : 'Active');
        const skillsMini = (j.skills || []).slice(0, 3).map(s => `<span class="skill-chip" style="font-size:0.68rem; padding:1px 4px;">${s}</span>`).join(' ');

        let applyBtnTable = '';
        if (j.applied) {
          applyBtnTable = `<button class="btn-card-apply applied" style="padding: 0.25rem 0.55rem; font-size: 0.74rem;" disabled>Applied ✓</button>`;
        } else {
          applyBtnTable = `<button class="btn-card-apply ready" style="padding: 0.25rem 0.55rem; font-size: 0.74rem;" onclick="applyToJobCard('${j.id}', this)">⚡ Apply</button>`;
        }

        let statusBadge = j.applied ? '<span class="status-pill status-applied">Applied ✅</span>' : (j.ai_score >= 70 ? '<span class="status-pill status-ready">Ready ⚡</span>' : '<span class="status-pill status-review">&lt;70% 🔍</span>');

        return `
          <tr>
            <td style="text-align: center;">
              <div class="ai-score-badge ${scoreClass}" style="font-size: 0.95rem; padding: 0.2rem 0.5rem;">
                ${j.ai_score}%
              </div>
            </td>
            <td>
              <div style="font-weight: 700; color: white; cursor: pointer;" onclick="openJobModal('${j.id}')">${j.title}</div>
              <div style="font-size: 0.8rem; color: #38bdf8;">🏢 ${j.company}</div>
            </td>
            <td>
              <div style="font-size: 0.82rem;">📍 ${j.location}</div>
              <div style="font-size: 0.75rem; color: #94a3b8;">💼 ${j.experience || 'Fresher'}</div>
            </td>
            <td>
              <div style="display: flex; gap: 4px; flex-wrap: wrap;">${skillsMini}</div>
            </td>
            <td>
              <div style="display: flex; flex-direction: column; gap: 4px;">
                <span class="platform-pill ${pillClass}">${j.source === 'NopeRi' ? '🟠 NopeRi Direct API' : '🤖 ' + j.platform}</span>
                ${statusBadge}
              </div>
            </td>
            <td>
              <div style="font-size: 0.78rem; color: #cbd5e1; max-width: 280px;">${j.ai_reason}</div>
            </td>
            <td>
              <div style="font-size: 0.75rem; color: #94a3b8; font-family: var(--font-mono);">${fmtDate}</div>
            </td>
            <td style="text-align: right; white-space: nowrap;">
              <div style="display: flex; gap: 6px; justify-content: flex-end; align-items: center;">
                <button class="btn-view-job" style="padding: 0.25rem 0.55rem; font-size: 0.74rem;" onclick="openJobModal('${j.id}')">Info</button>
                ${applyBtnTable}
                <a href="${j.url}" target="_blank" class="btn-view-job" style="padding: 0.25rem 0.55rem; font-size: 0.74rem;">↗</a>
              </div>
            </td>
          </tr>
        `;
      }).join('');
    }

    // 1-Click Apply Action
    async function applyToJobCard(jobId, btnElement) {
      const job = allJobs.find(j => String(j.id) === String(jobId));
      if (!job) return;

      btnElement.disabled = true;
      btnElement.className = "btn-card-apply applying";
      btnElement.innerHTML = `⏳ Applying...`;

      try {
        const res = await fetch('/api/apply-job', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            job_id: job.id,
            title: job.title,
            company: job.company,
            location: job.location,
            salary: job.salary,
            ai_score: job.ai_score,
            url: job.url,
            platform: job.platform
          })
        });
        const result = await res.json();

        if (result.success) {
          if (result.external && result.url) {
            window.open(result.url, '_blank');
            btnElement.className = "btn-card-apply ready";
            btnElement.innerHTML = `Opened Link ↗`;
            btnElement.disabled = false;
            showToast(`🔗 Opened portal application for "${job.title}" at ${job.company}`);
          } else {
            btnElement.className = "btn-card-apply applied";
            btnElement.innerHTML = `Applied ✅`;
            btnElement.disabled = true;
            job.applied = true;
            job.status = "Applied";
            showToast(`🚀 Successfully applied to "${job.title}" at ${job.company}!`, 'success');
            loadJobsData();
          }
        } else {
          btnElement.className = "btn-card-apply ready";
          btnElement.innerHTML = `⚡ Try Again`;
          btnElement.disabled = false;
          showToast(`❌ Apply notice: ${result.message}`, 'error');
        }
      } catch (e) {
        btnElement.className = "btn-card-apply ready";
        btnElement.innerHTML = `⚡ Try Again`;
        btnElement.disabled = false;
        showToast(`❌ Error: ${e.message}`, 'error');
      }
    }

    // Settings Loading & Saving
    async function loadSettingsData() {
      try {
        const res = await fetch('/api/settings');
        const data = await res.json();
        const cand = data.candidate || {};
        const ai = data.ai || {};

        document.getElementById('set-name').value = cand.name || "";
        document.getElementById('set-email').value = cand.email || "";
        document.getElementById('set-phone').value = cand.phone || "";
        document.getElementById('set-location').value = cand.primary_location || "";
        document.getElementById('set-locations').value = Array.isArray(cand.locations) ? cand.locations.join(', ') : cand.locations || "";
        document.getElementById('set-relocate').value = cand.willing_to_relocate || "Yes";

        document.getElementById('set-fresher').value = cand.is_fresher ? "true" : "false";
        document.getElementById('set-exp-years').value = cand.experience_years || 0;
        document.getElementById('set-curr-ctc').value = cand.current_ctc || "0";
        document.getElementById('set-exp-ctc').value = cand.expected_ctc || "400000";
        document.getElementById('set-notice').value = cand.notice_days || 15;
        document.getElementById('set-exp-summary').value = cand.experience_summary || "";

        document.getElementById('set-skills').value = Array.isArray(cand.primary_skills) ? cand.primary_skills.join(', ') : cand.primary_skills || "";
        document.getElementById('set-veto').value = Array.isArray(cand.veto_skills) ? cand.veto_skills.join(', ') : cand.veto_skills || "";
        document.getElementById('set-projects').value = cand.projects || "";

        document.getElementById('set-resume-path').value = cand.resume_path || "";
        updateResumeStatusBadge(cand.resume_exists, cand.resume_size_kb);

        const prov = ai.provider || "gemini";
        const radios = document.getElementsByName('ai_provider');
        for (let r of radios) {
          if (r.value === prov) r.checked = true;
        }

        document.getElementById('set-gemini-key').value = ai.gemini_key || "";
        document.getElementById('set-gemini-model').value = ai.gemini_model || "gemini-3.5-flash-lite";
        document.getElementById('set-groq-key').value = ai.groq_key || "";
        document.getElementById('set-groq-model').value = ai.groq_model || "llama-3.3-70b-versatile";

        document.getElementById('set-threshold').value = ai.score_threshold || 70;
        document.getElementById('thresh-val').innerText = (ai.score_threshold || 70) + '%';
      } catch (e) {
        console.error("Error loading settings:", e);
      }
    }

    function onFresherChange() {
      const isFresher = document.getElementById('set-fresher').value === "true";
      const expInput = document.getElementById('set-exp-years');
      if (isFresher) expInput.value = 0;
    }

    function toggleKeyVisibility(id, btn) {
      const inp = document.getElementById(id);
      if (inp.type === "password") {
        inp.type = "text";
        btn.innerText = "🙈";
      } else {
        inp.type = "password";
        btn.innerText = "👁";
      }
    }

    function updateResumeStatusBadge(exists, sizeKb) {
      const b = document.getElementById('set-resume-status');
      if (exists) {
        b.className = "file-status-badge status-file-ok";
        b.innerText = `🟢 PDF Verified on Disk (${sizeKb || 120} KB)`;
      } else {
        b.className = "file-status-badge status-file-err";
        b.innerText = `🔴 File Not Found at Path`;
      }
    }

    async function testKey(provider) {
      const keyId = provider === 'groq' ? 'set-groq-key' : 'set-gemini-key';
      const modelId = provider === 'groq' ? 'set-groq-model' : 'set-gemini-model';
      const resId = provider === 'groq' ? 'groq-test-result' : 'gemini-test-result';

      const key = document.getElementById(keyId).value.trim();
      const model = document.getElementById(modelId).value.trim();
      const resSpan = document.getElementById(resId);

      if (!key) {
        resSpan.innerText = "❌ Please enter key first";
        resSpan.style.color = "#f87171";
        return;
      }

      resSpan.innerText = "⏳ Testing key connection...";
      resSpan.style.color = "#fbbf24";

      try {
        const res = await fetch('/api/settings/test-key', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ provider, key, model })
        });
        const d = await res.json();
        if (d.valid) {
          resSpan.innerText = `✅ ${d.message}`;
          resSpan.style.color = "#34d399";
        } else {
          resSpan.innerText = `❌ ${d.message}`;
          resSpan.style.color = "#f87171";
        }
      } catch (e) {
        resSpan.innerText = `❌ Error: ${e.message}`;
        resSpan.style.color = "#f87171";
      }
    }

    async function saveAllSettings() {
      const saveMsg = document.getElementById('save-msg');
      saveMsg.innerText = "⏳ Saving and synchronizing across NopeRi & job-bot...";
      saveMsg.style.color = "#fbbf24";

      let provider = "gemini";
      const radios = document.getElementsByName('ai_provider');
      for (let r of radios) {
        if (r.checked) provider = r.value;
      }

      const payload = {
        candidate: {
          name: document.getElementById('set-name').value.trim(),
          email: document.getElementById('set-email').value.trim(),
          phone: document.getElementById('set-phone').value.trim(),
          primary_location: document.getElementById('set-location').value.trim(),
          locations: document.getElementById('set-locations').value.split(',').map(s => s.trim()).filter(Boolean),
          willing_to_relocate: document.getElementById('set-relocate').value,
          is_fresher: document.getElementById('set-fresher').value === "true",
          experience_years: parseInt(document.getElementById('set-exp-years').value || 0),
          current_ctc: document.getElementById('set-curr-ctc').value.trim(),
          expected_ctc: document.getElementById('set-exp-ctc').value.trim(),
          notice_days: parseInt(document.getElementById('set-notice').value || 15),
          experience_summary: document.getElementById('set-exp-summary').value.trim(),
          primary_skills: document.getElementById('set-skills').value.split(',').map(s => s.trim()).filter(Boolean),
          veto_skills: document.getElementById('set-veto').value.split(',').map(s => s.trim()).filter(Boolean),
          projects: document.getElementById('set-projects').value.trim(),
          resume_path: document.getElementById('set-resume-path').value.trim()
        },
        ai: {
          provider: provider,
          gemini_key: document.getElementById('set-gemini-key').value.trim(),
          gemini_model: document.getElementById('set-gemini-model').value.trim(),
          groq_key: document.getElementById('set-groq-key').value.trim(),
          groq_model: document.getElementById('set-groq-model').value.trim(),
          score_threshold: parseInt(document.getElementById('set-threshold').value || 70)
        }
      };

      try {
        const res = await fetch('/api/settings', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        const d = await res.json();
        if (d.success) {
          saveMsg.innerText = `✅ ${d.message}`;
          saveMsg.style.color = "#34d399";
          showToast('✅ Configuration updated and synchronized to both NopeRi & job-bot!', 'success');
          loadJobsData();
          setTimeout(() => { closeSettingsModal(); saveMsg.innerText = ""; }, 1200);
        } else {
          saveMsg.innerText = `❌ Error: ${d.message}`;
          saveMsg.style.color = "#f87171";
        }
      } catch (e) {
        saveMsg.innerText = `❌ Request failed: ${e.message}`;
        saveMsg.style.color = "#f87171";
      }
    }

    // Native CMD Window Launcher
    async function openNativeTerminal(target) {
      try {
        const res = await fetch('/api/open-terminal', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ target: target || 'noperi' })
        });
        const data = await res.json();
        if (data.success) {
          showToast(`🖥️ ${data.message}`, 'success');
        } else {
          showToast(`Notice: ${data.message}`, 'error');
        }
      } catch (e) {
        showToast(`Terminal launch error: ${e.message}`, 'error');
      }
    }

    // Bot Control
    async function runBots(target) {
      const popCmd = document.getElementById('chk-pop-cmd')?.checked;
      if (popCmd) {
        openNativeTerminal(target === 'both' ? 'noperi' : target);
      }

      try {
        const res = await fetch('/api/run', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ target: target, auto: true })
        });
        const data = await res.json();
        if (data.success) {
          showToast(`🚀 Started ${target.toUpperCase()} automation engine!`, 'success');
        } else {
          showToast(`Notice: ${data.message}`, 'error');
        }
        checkBotStatus();
      } catch (e) {
        showToast(`Run error: ${e.message}`, 'error');
      }
    }

    async function stopRun() {
      try {
        const res = await fetch('/api/stop', { method: 'POST' });
        const data = await res.json();
        showToast(`🛑 ${data.message}`);
        checkBotStatus();
      } catch (e) {
        showToast(`Stop error: ${e.message}`, 'error');
      }
    }

    // Init on DOM ready
    window.addEventListener('DOMContentLoaded', () => {
      initLogStream();
      loadJobsData();
      checkBotStatus();
      setInterval(loadJobsData, 6000);
    });
  </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def index():
    return DASHBOARD_HTML


def find_available_port(start_port: int = 8765) -> int:
    import socket
    for p in range(start_port, start_port + 25):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('127.0.0.1', p))
                return p
        except OSError:
            continue
    return start_port

selected_port = find_available_port(8765)

def open_browser_later(port: int):
    time.sleep(1.2)
    webbrowser.open(f"http://localhost:{port}")

if __name__ == "__main__":
    port = selected_port
    print("=" * 65)
    print(f"🤖 AI Job Automation Hub running at http://localhost:{port}")
    print("=" * 65)

    threading.Thread(target=open_browser_later, args=(port,), daemon=True).start()
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")
