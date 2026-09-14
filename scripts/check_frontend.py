"""Local browser acceptance checks; needs Playwright and a running demo API."""
import json
import os
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    output = Path("docs/reports")
    output.mkdir(parents=True, exist_ok=True)
    errors = []
    checks = {}
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1060}, device_scale_factor=1)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto(os.getenv("CHICAGO_TEST_URL", "http://127.0.0.1:8000"), wait_until="networkidle")
        page.wait_for_selector("#overview-kpis .kpi")
        checks["overview"] = page.locator("#overview-kpis .kpi").count() == 6
        checks["charts"] = page.locator("#overview .js-plotly-plot").count() == 3
        page.screenshot(path=str(output / "ui-overview-desktop.png"), full_page=True)
        before = page.locator("#overview-kpis .kpi-value").first.inner_text()
        page.select_option("#company", index=1)
        page.get_by_role("button", name="Apply filters").click()
        page.wait_for_function("document.querySelector('#content').getAttribute('aria-busy') === 'false'")
        checks["filters"] = page.locator("#overview-kpis .kpi-value").first.inner_text() != before
        page.get_by_role("button", name="Reset filters").click()
        page.get_by_role("link", name="Operations", exact=True).click()
        page.wait_for_selector("#trip-rows .trip-link")
        checks["operations"] = page.locator("#operations .js-plotly-plot").count() == 4
        page.screenshot(path=str(output / "ui-operations-desktop.png"), full_page=True)
        first = page.locator("#trip-rows .trip-link").first.inner_text()
        page.get_by_role("button", name="Next page").click()
        page.wait_for_function("document.querySelector('#content').getAttribute('aria-busy') === 'false'")
        checks["pagination"] = page.locator("#trip-rows .trip-link").first.inner_text() != first
        page.locator("#trip-rows .trip-link").first.click()
        page.wait_for_selector(".maplibregl-canvas")
        page.wait_for_function("state.map && state.map.loaded()", timeout=60000)
        checks["map_features"] = page.evaluate("state.geo.features.length > 0")
        checks["trip_detail"] = page.locator("#trip-detail dl").count() == 1
        checks["map_tiles"] = page.evaluate("state.map.isSourceLoaded('basemap')")
        page.get_by_role("button", name="Revenue by Area", exact=True).click()
        checks["area_mode"] = page.evaluate("state.map.getLayoutProperty('area-circles', 'visibility') === 'visible'")
        page.get_by_role("button", name="Pickup Density", exact=True).click()
        page.screenshot(path=str(output / "ui-geography-desktop.png"), full_page=True)
        page.get_by_role("button", name="At pickup").click()
        page.wait_for_function("!document.querySelector('#nearby').disabled")
        checks["optional_places"] = "unavailable" in page.locator("#place-results").inner_text()
        page.get_by_role("link", name="Data Quality", exact=True).click()
        page.wait_for_selector("#quality-kpis .kpi")
        checks["quality"] = page.locator("#quality-kpis .kpi").count() == 6
        checks["quality_filter_scope"] = page.locator("#company").is_disabled()
        page.screenshot(path=str(output / "ui-quality-desktop.png"), full_page=True)
        for width, height in [(390, 844), (768, 1024)]:
            page.set_viewport_size({"width": width, "height": height})
            for section in ("overview", "geography", "quality"):
                page.locator(f'nav a[data-page="{section}"]').click()
                page.wait_for_function("document.querySelector('#content').getAttribute('aria-busy') === 'false'")
                checks[f"no_overflow_{section}_{width}"] = page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                page.screenshot(path=str(output / f"ui-{section}-{width}.png"), full_page=True)
        checks["no_js_errors"] = not errors
        browser.close()
    (output / "frontend-checks.json").write_text(json.dumps({"checks": checks, "errors": errors}, indent=2), encoding="utf-8")
    print(json.dumps(checks, indent=2))
    if not all(checks.values()):
        raise SystemExit("Frontend checks failed")


if __name__ == "__main__":
    main()
