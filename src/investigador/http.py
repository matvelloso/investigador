from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib import parse, request


DEFAULT_TIMEOUT = 30
DEFAULT_HEADERS = {
    "User-Agent": "investigador/0.1 (+https://github.com/jxnxts/mcp-brasil inspired prototype)",
    "Accept": "*/*",
}


@dataclass(slots=True)
class HttpResponse:
    url: str
    status: int
    headers: dict[str, str]
    content: bytes

    def text(self, encoding: str | None = None) -> str:
        candidate = encoding or self._encoding_from_headers() or "utf-8"
        for option in (candidate, "utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                return self.content.decode(option)
            except UnicodeDecodeError:
                continue
        return self.content.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.text())

    def _encoding_from_headers(self) -> str | None:
        content_type = self.headers.get("content-type", "")
        marker = "charset="
        if marker in content_type:
            return content_type.split(marker, 1)[1].split(";", 1)[0].strip()
        return None


def build_url(url: str, query: dict[str, Any] | None = None) -> str:
    if not query:
        return url
    prepared: list[tuple[str, str]] = []
    for key, value in query.items():
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            for item in value:
                prepared.append((key, str(item)))
        else:
            prepared.append((key, str(value)))
    separator = "&" if parse.urlsplit(url).query else "?"
    return url + separator + parse.urlencode(prepared, doseq=True)


def fetch(
    url: str,
    *,
    query: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    method: str = "GET",
    data: bytes | None = None,
) -> HttpResponse:
    resolved_url = build_url(url, query)
    merged_headers = dict(DEFAULT_HEADERS)
    if headers:
        merged_headers.update(headers)
    req = request.Request(resolved_url, headers=merged_headers, method=method, data=data)
    with request.urlopen(req, timeout=timeout) as response:
        headers_map = {key.lower(): value for key, value in response.headers.items()}
        status = getattr(response, "status", response.getcode())
        return HttpResponse(
            url=response.geturl(),
            status=status,
            headers=headers_map,
            content=response.read(),
        )


def fetch_json(
    url: str,
    *,
    query: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    method: str = "GET",
    data: bytes | None = None,
) -> tuple[Any, HttpResponse]:
    response = fetch(url, query=query, headers=headers, timeout=timeout, method=method, data=data)
    return response.json(), response


def fetch_text(
    url: str,
    *,
    query: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> tuple[str, HttpResponse]:
    response = fetch(url, query=query, headers=headers, timeout=timeout)
    return response.text(), response


def fetch_bytes(
    url: str,
    *,
    query: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> HttpResponse:
    return fetch(url, query=query, headers=headers, timeout=timeout)
