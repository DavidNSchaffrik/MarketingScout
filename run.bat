@echo off
setlocal enabledelayedexpansion

echo Activating virtual environment...
call venv\Scripts\activate

set /p disc_count="How many discovery workers? "
set /p work_count="How many thread workers? "

set disc_id=0
set work_id=0

:discovery_loop
if %disc_id% GEQ %disc_count% goto workers
echo Starting Discovery %disc_id%...
start "Discovery %disc_id%" cmd /k "cd /d %CD% && call venv\Scripts\activate && py src\run.py discovery %disc_id% %disc_count%"
set /a disc_id=%disc_id%+1
timeout /t 5 /nobreak >nul
goto discovery_loop

:workers
if %work_id% GEQ %work_count% goto end
echo Starting Worker %work_id%...
start "Worker %work_id%" cmd /k "cd /d %CD% && call venv\Scripts\activate && py src\run.py worker %work_id%"
set /a work_id=%work_id%+1
timeout /t 5 /nobreak >nul
goto workers

:end
echo All processes started.
pause