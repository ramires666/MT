@echo off

setlocal

cd /d %~dp0

set PYTHONPATH=src

python -m bokeh serve src\bokeh_app --address 0.0.0.0 --port 5006 --allow-websocket-origin=localhost:5006 --allow-websocket-origin=127.0.0.1:5006

