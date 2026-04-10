"""HTTP/Playwright adapters that map remote pages to `SourceForecast` records."""

from __future__ import annotations

from datetime import date

import httpx
from playwright.sync_api import sync_playwright

from forecast_pipeline.models import SourceForecast

from forecast_pipeline.config import SOURCE_DEFINITIONS, SourceDefinition
from forecast_pipeline.adapters.html_payloads import (
    PagePayload,
    _extract_title,
    _normalize_whitespace,
    _strip_html,
)
from forecast_pipeline.adapters.parsers_remote_b import openmeteo_forecast_url
from forecast_pipeline.adapters.source_forecast_build import (
    _empty_result,
    _maybe_http_unavailable,
    _maybe_invalid_content,
    _maybe_wrong_location,
    _parsed_forecast_or_empty,
)


class BaseSourceAdapter:
    """Loads a page and converts it into a `SourceForecast`."""

    def __init__(self, definition: SourceDefinition, *, headed: bool = False) -> None:
        self.definition = definition
        self.headed = headed

    def page_to_result(
        self,
        page: PagePayload,
        *,
        fetched_at: str,
        target_date: date,
    ) -> SourceForecast:
        """Normalize fetched HTML or JSON into a structured source forecast."""

        for check in (
            _maybe_http_unavailable,
            _maybe_invalid_content,
            _maybe_wrong_location,
        ):
            early = check(
                self.definition, page, fetched_at=fetched_at, target_date=target_date
            )
            if early is not None:
                return early
        return _parsed_forecast_or_empty(
            self.definition, page, fetched_at=fetched_at, target_date=target_date
        )

    def error_result(
        self,
        *,
        fetched_at: str,
        target_date: date,
        exc: Exception,
    ) -> SourceForecast:
        """Return a structured error when the transport layer fails."""

        return _empty_result(
            self.definition,
            fetched_at=fetched_at,
            target_date=target_date,
            status="error",
            note=f"Quelle konnte nicht verarbeitet werden: {exc.__class__.__name__}",
        )

    def fetch(
        self,
        client: httpx.Client,
        *,
        target_date: date,
        fetched_at: str,
    ) -> SourceForecast:
        """Download the page with `client` and run `page_to_result`."""

        try:
            payload = self.load_page(client)
        except Exception as exc:
            return self.error_result(
                fetched_at=fetched_at, target_date=target_date, exc=exc
            )
        return self.page_to_result(
            payload, fetched_at=fetched_at, target_date=target_date
        )

    def load_page(self, client: httpx.Client) -> PagePayload:
        """Blocking fetch of raw page content (subclasses implement transport)."""

        return self._fetch_page(client)

    def _fetch_page(self, client: httpx.Client) -> PagePayload:
        raise NotImplementedError


class HttpSourceAdapter(BaseSourceAdapter):
    """Plain HTTP GET with HTML stripped to text for parsers."""

    def _fetch_page(self, client: httpx.Client) -> PagePayload:
        response = client.get(self.definition.source_url, follow_redirects=True)
        html = response.text
        return PagePayload(
            html=html,
            text=_strip_html(html),
            title=_extract_title(html),
            final_url=str(response.url),
            status_code=response.status_code,
        )


class OpenMeteoSourceAdapter(BaseSourceAdapter):
    """Fetches JSON from the Open-Meteo API (coordinates from config, not the page URL)."""

    def _fetch_page(self, client: httpx.Client) -> PagePayload:
        response = client.get(openmeteo_forecast_url(), follow_redirects=True)
        body = response.text
        return PagePayload(
            html=body,
            text=body,
            title="Open-Meteo",
            final_url=str(response.url),
            status_code=response.status_code,
        )


class YrApiSourceAdapter(BaseSourceAdapter):
    """Fetches JSON from the official yr.no location forecast API."""

    def _fetch_page(self, client: httpx.Client) -> PagePayload:
        response = client.get(
            self.definition.source_url,
            follow_redirects=True,
            headers={"Accept": "application/json"},
        )
        body = response.text
        return PagePayload(
            html=body,
            text=body,
            title="yr.no",
            final_url=str(response.url),
            status_code=response.status_code,
        )


def _playwright_page_payload(
    definition: SourceDefinition, *, headed: bool
) -> PagePayload:
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=not headed)
        page = browser.new_page(
            locale="de-DE",
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
        )
        response = page.goto(definition.source_url, wait_until="domcontentloaded")
        if definition.wait_for_ms:
            page.wait_for_timeout(definition.wait_for_ms)
        html = page.content()
        text = _normalize_whitespace(page.locator("body").inner_text())
        title = page.title()
        final_url = page.url
        status_code = response.status if response else None
        browser.close()
    return PagePayload(
        html=html,
        text=text,
        title=title,
        final_url=final_url,
        status_code=status_code,
    )


class PlaywrightSourceAdapter(BaseSourceAdapter):
    """Headed or headless Chromium fetch for script-heavy pages."""

    def _fetch_page(self, client: httpx.Client) -> PagePayload:
        del client
        return _playwright_page_payload(self.definition, headed=self.headed)


def build_source_adapters(*, headed: bool = False) -> list[BaseSourceAdapter]:
    """Instantiate one adapter per catalog entry."""

    adapters: list[BaseSourceAdapter] = []
    for definition in SOURCE_DEFINITIONS:
        if definition.fetch_mode == "playwright":
            adapters.append(PlaywrightSourceAdapter(definition, headed=headed))
        elif definition.fetch_mode == "openmeteo":
            adapters.append(OpenMeteoSourceAdapter(definition, headed=headed))
        elif definition.fetch_mode == "yr_api":
            adapters.append(YrApiSourceAdapter(definition, headed=headed))
        else:
            adapters.append(HttpSourceAdapter(definition, headed=headed))
    return adapters


_ADAPTER_PUBLIC_METHODS = (BaseSourceAdapter.fetch,)
