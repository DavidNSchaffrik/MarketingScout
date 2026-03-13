@echo off
echo Starting monitor server...
call venv\Scripts\activate
py src\monitor.py
pause
