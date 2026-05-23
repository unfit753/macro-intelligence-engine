"""Product-positioning guardrails for research-only macro intelligence."""
from __future__ import annotations


RESEARCH_POSITIONING = (
    "Macro intelligence and scenario research. Not personalized investment "
    "advice. Forecasts are probabilistic and may be wrong. No suitability "
    "assessment is performed. Users are responsible for their own decisions."
)

PUBLIC_REQUIRED_COPY: tuple[str, ...] = (
    "Macro intelligence and scenario research.",
    "Not personalized investment advice.",
    "Forecasts are probabilistic and may be wrong.",
    "No suitability assessment is performed.",
    "Users are responsible for their own decisions.",
)

FORBIDDEN_PUBLIC_PHRASES: tuple[str, ...] = (
    "guaranteed return",
    "guaranteed profit",
    "risk-free",
    "personalized portfolio advice",
    "suitable for you",
    "you should buy",
    "you should sell",
    "buy this now",
    "sell this now",
    "copy trade",
    "automated execution",
)


def validate_public_copy(text: str) -> list[str]:
    """Return compliance warnings for public-facing copy.

    This is a smoke test, not legal review. It catches wording that would push
    the commercial product away from research and toward advice/suitability.
    """
    lowered = text.lower()
    warnings = []
    for phrase in FORBIDDEN_PUBLIC_PHRASES:
        if phrase in lowered:
            warnings.append(f"Forbidden research-only phrase: {phrase}")
    return warnings


def public_disclaimer() -> str:
    return " ".join(PUBLIC_REQUIRED_COPY)
