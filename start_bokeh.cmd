@echo off

setlocal

cd /d %~dp0

for /f "tokens=5" %%p in ('netstat -ano ^| findstr /r /c:":5006 .*LISTENING"') do (
  taskkill /PID %%p /F >nul 2>nul
)

timeout /t 1 >nul
set PYTHONPATH=src

python -m bokeh serve src\bokeh_app --address localhost --port 5006 --allow-websocket-origin=localhost:5006 --allow-websocket-origin=127.0.0.1:5006
