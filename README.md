# ⚡ AI Job Automation Hub (NopeRi + job-bot)

> **Autonomous Multi-Platform Job Application Engine & Unified Real-Time Web Controller**  
> Powered by **Google Gemini 3.5 Flash-Lite** & **Groq Cloud Llama 3.3** • Strict $\ge 70\%$ AI Fit Filter • 1-Click Apply

---

## 🌟 Key Features

* **🎛️ Unified Command Center & Real-Time Web Dashboard:**  
  Single-pane control on `http://localhost:8765` featuring real-time Server-Sent Events (SSE) live streaming terminal logs, application metrics, filter tabs, and CSV export.
* **🚀 Dual Engine Architecture:**
  * **NopeRi (Naukri Direct CloudGateway API):** High-speed, headless direct API application with automatic JWT token renewal, RSA request signing, and intelligent questionnaire answering.
  * **job-bot (Multi-Platform Browser Automation):** Playwright engine supporting Naukri, Internshala, and Indeed.
* **🎯 Strict 70% AI Match Threshold:**  
  Guarantees that only vacancies with an AI match score $\ge 70\%$ are applied to automatically. Lower-scoring vacancies ($< 70\%$) are routed to manual review.
* **⚡ 1-Click Interactive "Apply Now" on Every Card:**  
  Review vacancies as interactive visual cards with match percentage, rationale, required skills, and click **`⚡ Apply Now`** to immediately dispatch an application via the API.
* **🔄 Live Settings Synchronization:**  
  Interactive web settings panel that synchronizes candidate contact info, CTC, notice period, resume PDF path, tech stack, and API keys across both `NopeRi` and `job-bot` in real time.
* **🔑 Dual AI Providers with In-App Latency Testing:**  
  Built-in support for **Google Gemini** (1,500 free requests/day) and **Groq Cloud** with live ping and credential verification.

---

## 📁 Repository Structure

```
├── launch_hub.py              # Unified FastAPI backend, controller & Single Page App UI
├── AI_Job_Hub.bat             # 1-Click Windows desktop launcher
├── run.bat                    # Quick terminal batch runner
├── requirements.txt           # Unified Python dependencies
├── .env.example               # Environment variables configuration template
├── .gitignore                 # Safe gitignore protecting credentials and databases
│
├── NopeRi/                    # Naukri Direct CloudGateway API Engine
│   ├── apply_agent.py         # Application pipeline & search runner
│   ├── config.json            # Profile, search queries, score threshold (70%)
│   ├── discovered_jobs.json   # Discovered vacancies & live status pool
│   ├── veto.json              # Keyword & company veto rules
│   └── src/                   # HTTP client, session manager, questionnaire AI
│
└── job-bot/                   # Playwright Multi-Platform Browser Engine
    ├── main.py                # Multi-platform runner
    ├── config.yaml            # Filters, applicant details, score threshold (70%)
    ├── platforms/             # Platform drivers (Naukri, Internshala, Indeed)
    └── utils/                 # Gemini/Groq AI engine, matcher, confirmation logic
```

---

## 🚀 Quick Start Guide

### 1. Prerequisites
* Python 3.10, 3.11, 3.12, or 3.13
* Active Naukri account credentials
* Google Gemini API Key ([Get free key](https://aistudio.google.com/app/apikey)) or Groq Cloud Key ([Get free key](https://console.groq.com/keys))

### 2. Installation
```bash
# Clone the repository
git clone https://github.com/<your-username>/<your-repo-name>.git
cd <your-repo-name>

# Install all unified dependencies
pip install -r requirements.txt

# Install Playwright browser binaries (for job-bot)
playwright install
```

### 3. Configuration
Copy `.env.example` to `.env` or simply configure everything via the Web UI Settings tab:
```bash
cp .env.example .env
```
Fill in your details in `.env`:
```env
NAUKRI_USERNAME=your_email_or_phone
PASSWORD=your_naukri_password
AI_PROVIDER=gemini
GEMINI_API_KEY=your_gemini_api_key
GEMINI_MODEL=gemini-3.5-flash-lite
RESUME_PATH=C:\path\to\your\resume.pdf
```

### 4. Run the Hub
Double-click **`run.bat`** (or **`AI_Job_Hub.bat`**), or run in terminal:
```bash
python launch_hub.py
```
Your default browser will automatically open **`http://localhost:8765`**.

---

## 📤 Git Upload Instructions

To upload this repository to your GitHub account:

```bash
# 1. Initialize git (if not already done)
git init

# 2. Stage all clean project files
git add .

# 3. Create initial commit
git commit -m "feat: unified AI Job Automation Hub with NopeRi, job-bot, and interactive web dashboard"

# 4. Set main branch
git branch -M main

# 5. Add your remote GitHub repository
git remote add origin https://github.com/<your-username>/<your-repo-name>.git

# 6. Push code to GitHub
git push -u origin main
```

---

## 🛡️ Security & Privacy
* **Zero Credentials Committed:** `.env`, session tokens, and local SQLite databases (`*.db`) are strictly excluded by `.gitignore`.
* **Safe Resumes:** Store your resume locally and reference its path; it is never pushed to public Git repositories.
