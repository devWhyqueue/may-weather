import json
from datetime import date

import httpx

from forecast_pipeline.config import SourceDefinition
from forecast_pipeline.adapters.html_payloads import PagePayload
from forecast_pipeline.sources import HttpSourceAdapter, PlaywrightSourceAdapter


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
        "include_in_common_horizon": True,
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
            <th>06:00</th>
            <th>12:00</th>
            <th>18:00</th>
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
        target_date=date.today(),
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
                  <thead><tr><th>06:00</th><th>12:00</th><th>18:00</th></tr></thead>
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

    monkeypatch.setattr("forecast_pipeline.sources.sync_playwright", lambda: FakeContextManager())

    result = adapter.fetch(
        httpx.Client(),
        target_date=date.today(),
        fetched_at="2026-04-09T12:00:00Z",
    )
    assert result.status == "available"
    assert result.dayparts.evening.condition_summary == "Sonnig"


def test_weathercom_tenday_embedded_json() -> None:
    adapter = HttpSourceAdapter(
        make_definition(
            source_id="weathercom",
            location_markers=("Haltern am See",),
        )
    )
    blob = (
        'getSunV3DailyForecastWithHeadersUrlConfig":{'
        '\\"moonriseTimeLocal\\":[\\"2026-04-10T03:00:00+0200\\",'
        '\\"2026-04-11T03:00:00+0200\\",\\"2026-04-12T03:00:00+0200\\"],'
        '\\"calendarDayTemperatureMax\\":[18,19,20],'
        '\\"calendarDayTemperatureMin\\":[8,9,10],'
        '\\"precipChance\\":[20,30,null]'
    )
    result = adapter.page_to_result(
        PagePayload(
            html=blob,
            text=blob + " Haltern am See",
            title="Haltern am See Wetter",
        final_url="https://weather.com/de-DE/wetter/10tage/l/x",
            status_code=200,
        ),
        fetched_at="2026-04-09T12:00:00Z",
        target_date=date(2026, 4, 11),
    )
    assert result.status == "available"
    assert result.ranking_eligible is True
    assert result.dayparts.afternoon.temperature_celsius == 19.0


def test_weatherandradar_parses_day_interval_json() -> None:
    adapter = HttpSourceAdapter(
        make_definition(
            source_id="weatherandradar",
            location_markers=("Haltern am See",),
        )
    )
    interval_m = (
        '{"air_pressure":{"hpa":"1014"},"air_temperature":{"celsius":10,"fahrenheit":50},'
        '"date":"2026-04-24T00:00:00+02:00","precipitation":{"probability":0.25},'
        '"symbol":"mw____","type":"morning"}'
    )
    interval_a = (
        '{"air_pressure":{"hpa":"1014"},"air_temperature":{"celsius":14,"fahrenheit":57},'
        '"date":"2026-04-24T00:00:00+02:00","precipitation":{"probability":0.15},'
        '"symbol":"mw____","type":"afternoon"}'
    )
    interval_e = (
        '{"air_pressure":{"hpa":"1014"},"air_temperature":{"celsius":11,"fahrenheit":52},'
        '"date":"2026-04-24T00:00:00+02:00","precipitation":{"probability":0.2},'
        '"symbol":"mw____","type":"evening"}'
    )
    html = f"<html><body>x{interval_m},{interval_a},{interval_e}Haltern am See</body></html>"
    result = adapter.page_to_result(
        PagePayload(
            html=html,
            text="Haltern am See",
            title="Haltern am See",
            final_url="https://example.com/war",
            status_code=200,
        ),
        fetched_at="2026-04-09T12:00:00Z",
        target_date=date(2026, 4, 24),
    )
    assert result.status == "available"
    assert result.ranking_eligible is True
    assert result.dayparts.morning.temperature_celsius == 10.0


def test_yr_api_json_parser() -> None:
    adapter = HttpSourceAdapter(
        make_definition(
            source_id="yr",
            location_markers=(),
            invalid_markers=(),
        )
    )
    payload = {
        "shortIntervals": [
            {
                "start": "2026-04-10T08:00:00+02:00",
                "end": "2026-04-10T09:00:00+02:00",
                "symbolCode": {"next1Hour": "partlycloudy_day"},
                "precipitation": {"value": 0},
                "temperature": {"value": 12.0},
                "uvIndex": {"value": 2},
                "cloudCover": {"value": 40},
            },
            {
                "start": "2026-04-10T14:00:00+02:00",
                "end": "2026-04-10T15:00:00+02:00",
                "symbolCode": {"next1Hour": "clearsky_day"},
                "precipitation": {"value": 0},
                "temperature": {"value": 16.0},
                "uvIndex": {"value": 4},
                "cloudCover": {"value": 10},
            },
            {
                "start": "2026-04-10T19:00:00+02:00",
                "end": "2026-04-10T20:00:00+02:00",
                "symbolCode": {"next1Hour": "clearsky_night"},
                "precipitation": {"value": 0},
                "temperature": {"value": 11.0},
                "uvIndex": {"value": 0},
                "cloudCover": {"value": 5},
            },
        ]
    }
    raw = json.dumps(payload)
    result = adapter.page_to_result(
        PagePayload(
            html=raw,
            text=raw,
            title="yr",
            final_url="https://www.yr.no/api/v0/locations/2-2911396/forecast",
            status_code=200,
        ),
        fetched_at="2026-04-09T12:00:00Z",
        target_date=date(2026, 4, 10),
    )
    assert result.status == "available"
    assert result.ranking_eligible is True
    assert result.dayparts.afternoon.temperature_celsius is not None


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
