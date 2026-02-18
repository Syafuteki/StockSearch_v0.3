from datetime import date

from jpswing.ingest.edinet_client import EdinetClient


class _DummyResponse:
    def __init__(
        self,
        status_code: int = 200,
        payload: dict | None = None,
        headers: dict | None = None,
        content: bytes = b"",
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {"results": []}
        self.headers = headers or {}
        self.content = content
        self.text = text

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"status={self.status_code}")


def test_fetch_documents_list_sends_subscription_key_in_header_only(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_get(url, *, params=None, headers=None, timeout=None, follow_redirects=None):  # noqa: ANN001
        captured["url"] = url
        captured["params"] = params
        captured["headers"] = headers
        captured["timeout"] = timeout
        captured["follow_redirects"] = follow_redirects
        return _DummyResponse(status_code=200, payload={"results": [{"docID": "x"}]})

    monkeypatch.setattr("httpx.get", _fake_get)
    client = EdinetClient(base_url="https://disclosure2.edinet-fsa.go.jp", api_key="abc123", timeout_sec=30)
    rows = client.fetch_documents_list(date(2026, 2, 13))

    assert len(rows) == 1
    assert isinstance(captured.get("params"), dict)
    assert captured["params"]["date"] == "2026-02-13"  # type: ignore[index]
    assert captured["params"].get("Subscription-Key") is None  # type: ignore[index]
    assert isinstance(captured.get("headers"), dict)
    assert captured["headers"]["Subscription-Key"] == "abc123"  # type: ignore[index]


def test_fetch_documents_list_respects_retry_after_on_429(monkeypatch) -> None:
    calls = {"n": 0}
    slept: list[float] = []

    def _fake_retry(func, **kwargs):  # noqa: ANN001
        try:
            return func()
        except Exception:
            return func()

    def _fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    def _fake_get(url, *, params=None, headers=None, timeout=None, follow_redirects=None):  # noqa: ANN001, ARG001
        calls["n"] += 1
        if calls["n"] == 1:
            return _DummyResponse(status_code=429, headers={"Retry-After": "0.25"})
        return _DummyResponse(status_code=200, payload={"results": [{"docID": "x"}]})

    monkeypatch.setattr("jpswing.ingest.edinet_client.retry_with_backoff", _fake_retry)
    monkeypatch.setattr("jpswing.ingest.edinet_client.time.sleep", _fake_sleep)
    monkeypatch.setattr("httpx.get", _fake_get)
    client = EdinetClient(base_url="https://disclosure2.edinet-fsa.go.jp", api_key="abc123", timeout_sec=30)
    rows = client.fetch_documents_list(date(2026, 2, 13))

    assert len(rows) == 1
    assert calls["n"] >= 2
    assert slept
    assert slept[0] >= 0.25


def test_download_document_respects_retry_after_on_429(monkeypatch) -> None:
    calls = {"n": 0}
    slept: list[float] = []

    def _fake_retry(func, **kwargs):  # noqa: ANN001
        try:
            return func()
        except Exception:
            return func()

    def _fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    def _fake_get(url, *, params=None, headers=None, timeout=None, follow_redirects=None):  # noqa: ANN001, ARG001
        calls["n"] += 1
        if calls["n"] == 1:
            return _DummyResponse(status_code=429, headers={"Retry-After": "0.4"})
        return _DummyResponse(status_code=200, content=b"dummy")

    monkeypatch.setattr("jpswing.ingest.edinet_client.retry_with_backoff", _fake_retry)
    monkeypatch.setattr("jpswing.ingest.edinet_client.time.sleep", _fake_sleep)
    monkeypatch.setattr("httpx.get", _fake_get)
    client = EdinetClient(base_url="https://disclosure2.edinet-fsa.go.jp", api_key="abc123", timeout_sec=30)
    payload = client.download_document("S100TEST", file_type=5)

    assert payload == b"dummy"
    assert calls["n"] >= 2
    assert slept
    assert slept[0] >= 0.4
