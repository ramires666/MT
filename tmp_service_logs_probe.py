import os
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={'width': 1600, 'height': 1200})
    page.goto('http://localhost:5020/bokeh_app', wait_until='domcontentloaded', timeout=30000)
    page.wait_for_timeout(3000)
    btn = page.get_by_role('button', name='Service Logs (6)')
    if btn.count() == 0:
        btn = page.get_by_role('button', name='Service Logs (5)')
    if btn.count() == 0:
        btn = page.get_by_role('button', name='Service Logs')
    print('btn_count', btn.count(), flush=True)
    if btn.count():
        print('before_class', btn.first.get_attribute('class'), flush=True)
        btn.first.click(timeout=5000)
        page.wait_for_timeout(1500)
        print('after_class', btn.first.get_attribute('class'), flush=True)
        print('pre_count', page.locator('pre').count(), flush=True)
        if page.locator('pre').count():
            print('pre_text_prefix', page.locator('pre').first.inner_text()[:120], flush=True)
    os._exit(0)
