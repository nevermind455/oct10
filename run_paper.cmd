@echo off
rem Paper bot with the dashboard, on this Windows PC (live runs on the VPS).
rem Ctrl+C stops it cleanly: the final report prints, then this window waits.
cd /d "%~dp0"
python run.py --paper --dash
echo.
echo Bot stopped (exit code %ERRORLEVEL%). The final report is above; bot_exit.log has the reason.
pause
