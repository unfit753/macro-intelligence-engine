"""Small Ollama client used by optional local-model experiments."""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any

import requests


DEFAULT_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434").rstrip("/")
DEFAULT_MODEL = os.getenv("LOCAL_ORACLE_MODEL", "qwen3:4b-instruct")
DEFAULT_KEEP_ALIVE = os.getenv("LOCAL_ORACLE_KEEP_ALIVE", "30s")
DEFAULT_NUM_CTX = int(os.getenv("LOCAL_ORACLE_NUM_CTX", "4096"))


class OllamaError(RuntimeError):
    pass


@dataclass(frozen=True)
class OllamaResult:
    model: str
    content: str
    parsed: dict[str, Any] | None
    latency_ms: int
    raw: dict[str, Any]


def generate_json(
    prompt: str,
    *,
    system: str,
    model: str = DEFAULT_MODEL,
    host: str = DEFAULT_HOST,
    schema: dict[str, Any] | None = None,
    temperature: float = 0.1,
    num_ctx: int = DEFAULT_NUM_CTX,
    keep_alive: str = DEFAULT_KEEP_ALIVE,
    timeout_s: int = 180,
) -> OllamaResult:
    """Ask Ollama for one non-streaming JSON response."""
    payload: dict[str, Any] = {
        "model": model,
        "system": system,
        "prompt": prompt,
        "stream": False,
        "format": schema or "json",
        "keep_alive": keep_alive,
        "options": {
            "temperature": temperature,
            "num_ctx": num_ctx,
        },
    }
    t0 = time.time()
    try:
        response = requests.post(f"{host.rstrip('/')}/api/generate", json=payload, timeout=timeout_s)
    except requests.RequestException as exc:
        raise OllamaError(f"Ollama request failed: {exc}") from exc
    latency_ms = int((time.time() - t0) * 1000)
    if response.status_code >= 400:
        raise OllamaError(f"Ollama returned HTTP {response.status_code}: {response.text[:500]}")
    raw = response.json()
    content = str(raw.get("response") or "").strip()
    parsed: dict[str, Any] | None = None
    if content:
        try:
            obj = json.loads(content)
            parsed = obj if isinstance(obj, dict) else None
        except json.JSONDecodeError:
            parsed = None
    return OllamaResult(model=model, content=content, parsed=parsed, latency_ms=latency_ms, raw=raw)


def unload_model(model: str = DEFAULT_MODEL, host: str = DEFAULT_HOST, timeout_s: int = 30) -> None:
    """Tell Ollama to unload a model from memory."""
    payload = {"model": model, "prompt": "", "stream": False, "keep_alive": 0}
    try:
        requests.post(f"{host.rstrip('/')}/api/generate", json=payload, timeout=timeout_s)
    except requests.RequestException:
        pass

