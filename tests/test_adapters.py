import json
from datetime import date

import httpx

from forecast_pipeline.config import SourceDefinition
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
            <td><img alt="bedeckt"><span class="prob-line">20 %</span> 11 °C</td>
            <td><img alt="Teilweise wolkig"><span class="prob-line">10 %</span> 14 °C</td>
            <td><img alt="klar"><span class="prob-line">0 %</span> 10 °C</td>
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
    assert result.ranking_eligible is True
    assert result.dayparts.morning.condition_summary == "Bewölkt"
    assert result.dayparts.afternoon.precip_probability_pct == 10.0
    assert result.dayparts.evening.sunshine_hours == 1.0
    assert result.dayparts.morning.temperature_celsius == 11.0


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
                    <td><img alt="bedeckt"><span class="prob-line">20 %</span> 11 °C</td>
                    <td><img alt="Teilweise wolkig"><span class="prob-line">10 %</span> 14 °C</td>
                    <td><img alt="klar"><span class="prob-line">0 %</span> 10 °C</td>
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


def test_openmeteo_parser_hourly_to_dayparts() -> None:
    adapter = HttpSourceAdapter(
        make_definition(
            source_id="openmeteo",
            location_markers=(),
            invalid_markers=(),
        )
    )
    payload = {
        "hourly": {
            "time": [
                "2026-04-10T06:00",
                "2026-04-10T09:00",
                "2026-04-10T12:00",
                "2026-04-10T15:00",
                "2026-04-10T18:00",
                "2026-04-10T21:00",
            ],
            "temperature_2m": [8.0, 10.0, 18.0, 20.0, 16.0, 12.0],
            "precipitation_probability": [5, 5, 10, 15, 20, 10],
            "weather_code": [1, 1, 0, 2, 3, 2],
        }
    }
    raw = json.dumps(payload)
    result = adapter.page_to_result(
        PagePayload(
            html=raw,
            text=raw,
            title="Open-Meteo",
            final_url="https://api.open-meteo.com/v1/forecast",
            status_code=200,
        ),
        fetched_at="2026-04-09T12:00:00Z",
        target_date=date(2026, 4, 10),
    )
    assert result.status == "available"
    assert result.ranking_eligible is True
    assert result.dayparts.morning.temperature_celsius is not None
    assert result.dayparts.afternoon.temperature_celsius is not None
    assert result.dayparts.evening.temperature_celsius is not None
