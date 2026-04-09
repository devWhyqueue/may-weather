from datetime import date

import httpx

from forecast_pipeline.adapters.base import SourceDefinition
from forecast_pipeline.adapters.sources import HttpSourceAdapter, PagePayload, PlaywrightSourceAdapter


def make_definition(**overrides) -> SourceDefinition:
    payload = {
        "source_id": "ventusky",
        "source_name": "Ventusky",
        "source_url": "https://example.com/weather",
        "method": "html",
        "weight": 0.8,
        "horizon_days": 10,
        "fetch_mode": "http",
        "language": "de",
        "location_markers": ("Haltern am See",),
        "invalid_markers": (),
    }
    payload.update(overrides)
    return SourceDefinition(**payload)


def test_http_adapter_marks_404_as_unavailable() -> None:
    adapter = HttpSourceAdapter(make_definition())
    result = adapter.page_to_result(
        PagePayload(
            html="<html><title>404</title></html>",
            text="Not found",
            title="404",
            final_url="https://example.com/weather",
            status_code=404,
        ),
        fetched_at="2026-04-09T12:00:00Z",
        target_date=date(2026, 4, 10),
    )
    assert result.status == "unavailable"


def test_http_adapter_marks_access_denied_as_unavailable() -> None:
    adapter = HttpSourceAdapter(make_definition(invalid_markers=("access denied",)))
    result = adapter.page_to_result(
        PagePayload(
            html="<html><title>Access Denied</title></html>",
            text="Access Denied",
            title="Access Denied",
            final_url="https://example.com/weather",
            status_code=200,
        ),
        fetched_at="2026-04-09T12:00:00Z",
        target_date=date(2026, 4, 10),
    )
    assert result.status == "unavailable"


def test_http_adapter_extracts_dayparts_from_ventusky_table() -> None:
    adapter = HttpSourceAdapter(make_definition())
    html = """
    <html><title>Haltern am See</title><body>
      <div id="forecast_24">
        <table class="mesto-predpoved">
          <thead><tr>
            <th>06:00 <span>morgen</span></th>
            <th>12:00 <span>morgen</span></th>
            <th>18:00 <span>morgen</span></th>
          </tr></thead>
          <tbody><tr>
            <td><img alt="bedeckt"><span class="prob-line">20 %</span></td>
            <td><img alt="Teilweise wolkig"><span class="prob-line">10 %</span></td>
            <td><img alt="klar"><span class="prob-line">0 %</span></td>
          </tr></tbody>
        </table>
      </div>
      Haltern am See
    </body></html>
    """
    result = adapter.page_to_result(
        PagePayload(
            html=html,
            text="Haltern am See",
            title="Haltern am See",
            final_url="https://example.com/weather",
            status_code=200,
        ),
        fetched_at="2026-04-09T12:00:00Z",
        target_date=date(2026, 4, 10),
    )
    assert result.status == "available"
    assert result.dayparts.morning.condition_summary == "Bewölkt"
    assert result.dayparts.afternoon.precip_probability_pct == 10.0
    assert result.dayparts.evening.sunshine_hours == 1.0


def test_http_adapter_converts_fetch_exceptions_to_error(monkeypatch) -> None:
    adapter = HttpSourceAdapter(make_definition())

    def broken_get(*args, **kwargs):
        raise httpx.ReadTimeout("boom")

    client = httpx.Client()
    monkeypatch.setattr(client, "get", broken_get)
    result = adapter.fetch(
        client,
        target_date=date(2026, 4, 10),
        fetched_at="2026-04-09T12:00:00Z",
    )
    assert result.status == "error"


def test_playwright_adapter_smoke(monkeypatch) -> None:
    adapter = PlaywrightSourceAdapter(make_definition(fetch_mode="playwright", wait_for_ms=1))

    class FakeResponse:
        status = 200

    class FakeLocator:
        def inner_text(self) -> str:
            return "Haltern am See"

    class FakePage:
        url = "https://example.com/weather"

        def goto(self, *args, **kwargs):
            return FakeResponse()

        def wait_for_timeout(self, value: int) -> None:
            assert value == 1

        def content(self) -> str:
            return """
            <html><title>Haltern am See</title><body>
              <div id="forecast_24">
                <table class="mesto-predpoved">
                  <thead><tr><th>06:00 <span>morgen</span></th><th>12:00 <span>morgen</span></th><th>18:00 <span>morgen</span></th></tr></thead>
                  <tbody><tr>
                    <td><img alt="bedeckt"><span class="prob-line">20 %</span></td>
                    <td><img alt="Teilweise wolkig"><span class="prob-line">10 %</span></td>
                    <td><img alt="klar"><span class="prob-line">0 %</span></td>
                  </tr></tbody>
                </table>
              </div>
              Haltern am See
            </body></html>
            """

        def locator(self, selector: str) -> FakeLocator:
            assert selector == "body"
            return FakeLocator()

        def title(self) -> str:
            return "Haltern am See"

    class FakeBrowser:
        def new_page(self, **kwargs) -> FakePage:
            return FakePage()

        def close(self) -> None:
            return None

    class FakePlaywright:
        class chromium:
            @staticmethod
            def launch(headless: bool) -> FakeBrowser:
                assert headless is True
                return FakeBrowser()

    class FakeContextManager:
        def __enter__(self) -> FakePlaywright:
            return FakePlaywright()

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    monkeypatch.setattr("forecast_pipeline.adapters.sources.sync_playwright", lambda: FakeContextManager())

    result = adapter.fetch(
        httpx.Client(),
        target_date=date(2026, 4, 10),
        fetched_at="2026-04-09T12:00:00Z",
    )
    assert result.status == "available"
    assert result.dayparts.evening.condition_summary == "Sonnig"
