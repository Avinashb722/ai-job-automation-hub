@echo off
setlocal
cd /d "%~dp0"
title NopeRi - Naukri Auto-Apply Agent [Live Console]
color 0A
echo =====================================================================
echo                NopeRi (Naukri Direct API Engine)
echo             Unified Desktop Job Automation Hub Console
echo =====================================================================
echo.
echo [INFO] Working Directory: %~dp0
echo [INFO] Running Naukri automated application agent (--auto)...
echo.
python -u apply_agent.py --auto
if errorlevel 1 (
    echo.
    echo [FALLBACK] Attempting with WindowsApps Python launcher...
    "C:\Users\Hp\AppData\Local\Microsoft\WindowsApps\python.exe" -u apply_agent.py --auto
)
echo.
echo =====================================================================
echo Execution finished. Press any key to exit this console.
echo =====================================================================
pause
