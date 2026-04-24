"""Shared helpers for local HTTP-based chatbot regression scripts."""

from __future__ import annotations

import os
import sys
import time
import uuid
from typing import Any

import requests

BASE_URL = os.getenv("CHATBOT_BASE_URL", "http://localhost:8000")
REQUEST_SPACING_SECONDS = float(os.getenv("CHATBOT_REQUEST_SPACING_SECONDS", "6.5"))
RETRY_SLEEP_SECONDS = float(os.getenv("CHATBOT_RETRY_SLEEP_SECONDS", "65"))
MAX_RETRIES = int(os.getenv("CHATBOT_MAX_RETRIES", "1"))

_LAST_REQUEST_AT = 0.0


def _sleep_for_spacing() -> None:
    global _LAST_REQUEST_AT
    now = time.time()
    delta = now - _LAST_REQUEST_AT
    if delta < REQUEST_SPACING_SECONDS:
        time.sleep(REQUEST_SPACING_SECONDS - delta)
    _LAST_REQUEST_AT = time.time()


def _retry_sleep_seconds(response: requests.Response | None) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass
    return RETRY_SLEEP_SECONDS


def chat_json(
    question: str,
    session_id: str | None = None,
    *,
    timeout: float = 120,
    base_url: str = BASE_URL,
    max_retries: int = MAX_RETRIES,
) -> dict[str, Any]:
    sid = session_id or str(uuid.uuid4())
    payload = {"session_id": sid, "question": question}
    last_response: requests.Response | None = None
    error_text = ""
    started = time.time()

    for attempt in range(max_retries + 1):
        _sleep_for_spacing()
        request_started = time.time()
        try:
            response = requests.post(f"{base_url}/chat", json=payload, timeout=timeout)
            elapsed = time.time() - request_started
            last_response = response
            result = {
                "session_id": sid,
                "status": response.status_code,
                "elapsed": elapsed,
                "question": question,
            }
            if response.status_code == 200:
                result.update(response.json())
                return result
            error_text = response.text[:500]
            result["error"] = error_text
            if response.status_code == 429 and attempt < max_retries:
                time.sleep(_retry_sleep_seconds(response))
                continue
            return result
        except Exception as exc:  # pragma: no cover - network exceptions vary
            elapsed = time.time() - request_started
            error_text = str(exc)
            if attempt < max_retries:
                time.sleep(RETRY_SLEEP_SECONDS)
                continue
            return {
                "session_id": sid,
                "status": -1,
                "elapsed": elapsed,
                "question": question,
                "error": error_text,
            }

    return {
        "session_id": sid,
        "status": getattr(last_response, "status_code", -1),
        "elapsed": time.time() - started,
        "question": question,
        "error": error_text or "Unknown error",
    }


def fail_exit_code(statuses: list[str]) -> int:
    return 1 if any(status != "PASS" for status in statuses) else 0


def exit_for_results(results: list[tuple[Any, ...]]) -> None:
    statuses = [str(result[0]) for result in results]
    sys.exit(fail_exit_code(statuses))
