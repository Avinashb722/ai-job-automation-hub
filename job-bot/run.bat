@echo off
setlocal
cd /d "%~dp0"
title job-bot - Multi-Portal Auto-Apply Agent [Live Console]
color 0B
echo =====================================================================
echo            job-bot (Multi-Portal Automated Application Engine)
echo             Unified Desktop Job Automation Hub Console
echo =====================================================================
echo.
echo [INFO] Working Directory: %~dp0
echo [INFO] Running Multi-Portal automated application agent (--auto)...
echo.
python -u main.py --auto
if errorlevel 1 (
    echo.
    echo [FALLBACK] Attempting with WindowsApps Python launcher...
    "C:\Users\Hp\AppData\Local\Microsoft\WindowsApps\python.exe" -u main.py --auto
)
echo.
echo =====================================================================
echo Execution finished. Press any key to exit this console.
echo =====================================================================
pause
