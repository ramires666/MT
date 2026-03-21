import os
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={'width': 1600, 'height': 1000})
    page.goto('http://localhost:5019/bokeh_app', wait_until='domcontentloaded', timeout=30000)
    page.wait_for_timeout(3000)
    btn = page.get_by_role('button', name='Optimization Results')
    print('before_class', btn.get_attribute('class'), flush=True)
    btn.click(timeout=5000)
    page.wait_for_timeout(1000)
    print('after_class', btn.get_attribute('class'), flush=True)
    print('run_opt_count', page.get_by_role('button', name='Run Optimization').count(), flush=True)
    os._exit(0)
