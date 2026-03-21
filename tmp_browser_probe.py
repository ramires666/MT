import os
from playwright.sync_api import sync_playwright
msgs = []
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={'width': 1600, 'height': 1000})
    page.on('console', lambda msg: msgs.append(f"console:{msg.type}: {msg.text}"))
    page.on('pageerror', lambda exc: msgs.append(f"pageerror: {exc}"))
    resp = page.goto('http://localhost:5006/bokeh_app', wait_until='domcontentloaded', timeout=30000)
    page.wait_for_timeout(4000)
    print('status', resp.status if resp else None, flush=True)
    print('title', page.title(), flush=True)
    print('console_count', len(msgs), flush=True)
    for m in msgs:
        print(m, flush=True)
    os._exit(0)
