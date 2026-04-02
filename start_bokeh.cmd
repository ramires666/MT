@echo off

setlocal EnableDelayedExpansion

cd /d %~dp0

if not defined MT_SERVICE_BOKEH_PORT set MT_SERVICE_BOKEH_PORT=5006
if not defined BOKEH_SESSION_TOKEN_EXPIRATION set BOKEH_SESSION_TOKEN_EXPIRATION=86400

wsl.exe bash -lc "fuser -k %MT_SERVICE_BOKEH_PORT%/tcp >/dev/null 2>&1 || true" >nul 2>nul

for /f %%p in ('powershell -NoProfile -Command "$port=[int]$env:MT_SERVICE_BOKEH_PORT; Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue ^| Select-Object -ExpandProperty OwningProcess -Unique"') do (
  taskkill /PID %%p /F >nul 2>nul
)

timeout /t 1 >nul
set PYTHONPATH=src

set "BOKEH_PORT=%MT_SERVICE_BOKEH_PORT%"
call :find_free_port "%BOKEH_PORT%"
if not defined FREE_BOKEH_PORT (
  echo Failed to find a free Bokeh port starting from %MT_SERVICE_BOKEH_PORT%.
  exit /b 1
)

echo Starting Bokeh on port !FREE_BOKEH_PORT!...
if not "!FREE_BOKEH_PORT!"=="%MT_SERVICE_BOKEH_PORT%" (
  echo Port %MT_SERVICE_BOKEH_PORT% is busy. Falling back to !FREE_BOKEH_PORT!.
)
python -m bokeh serve src\bokeh_app --address localhost --port !FREE_BOKEH_PORT! --session-token-expiration=%BOKEH_SESSION_TOKEN_EXPIRATION% --allow-websocket-origin=localhost:!FREE_BOKEH_PORT! --allow-websocket-origin=127.0.0.1:!FREE_BOKEH_PORT!
exit /b %ERRORLEVEL%

:find_free_port
set "FREE_BOKEH_PORT="
set "_candidate=%~1"
if not defined _candidate set "_candidate=5006"
for /f %%p in ('powershell -NoProfile -Command "$start=[int]'!_candidate!'; $free=$null; for($p=$start; $p -lt ($start + 50); $p++){ try { $listener=[System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $p); $listener.Start(); $listener.Stop(); $free=$p; break } catch { } }; if($null -ne $free){ $free }"') do (
  set "FREE_BOKEH_PORT=%%p"
)
if defined FREE_BOKEH_PORT exit /b 0
exit /b 1
