@echo off
setlocal
title AI Job Automation Hub (NopeRi + job-bot)
cd /d "%~dp0"

echo =====================================================================
echo           AI JOB AUTOMATION HUB (NopeRi + job-bot)
echo           Candidate: Avinash Biradar ^| Fresher (0 Exp) ^| CTC: 4 LPA
echo           AI Engine: Google Gemini 3.5 Flash-Lite (1,500 req/day)
echo =====================================================================
echo.
echo Starting Unified Hub Web Dashboard on http://localhost:8765...
echo Opening Web UI in your default browser...
echo.

python launch_hub.py
if errorlevel 1 (
    echo.
    echo Trying fallback Python launcher...
    "C:\Users\Hp\AppData\Local\Microsoft\WindowsApps\python.exe" launch_hub.py
)

pause
