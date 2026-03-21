import os
from playwright.sync_api import sync_playwright
msgs = []
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={'width': 1600, 'height': 1200})
    page.on('console', lambda msg: msgs.append(f"console:{msg.type}: {msg.text}"))
    page.on('pageerror', lambda exc: msgs.append(f"pageerror: {exc}"))
    page.goto('http://localhost:5021/bokeh_app', wait_until='domcontentloaded', timeout=30000)
    page.wait_for_timeout(3000)
    btn = page.get_by_role('button', name='WFA')
    print('btn_count', btn.count(), flush=True)
    print('before_class', btn.first.get_attribute('class'), flush=True)
    btn.first.click(timeout=5000)
    page.wait_for_timeout(3000)
    print('after_class', btn.first.get_attribute('class'), flush=True)
    print('run_wfa_count', page.get_by_role('button', name='Run WFA').count(), flush=True)
    print('wfa_title_count', page.locator('text=WFA Stitched Equity').count(), flush=True)
    print('console_count', len(msgs), flush=True)
    for m in msgs:
        print(m, flush=True)
    os._exit(0)
