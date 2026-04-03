"""Cost calculator module — computes API costs from token usage and pricing."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class BatchCost:
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float


@dataclass
class SessionCost:
    total_prompt_tokens: int
    total_completion_tokens: int
    total_cost_usd: float


def calculate_batch_cost(
    usage: dict,
    prompt_price: float,
    completion_price: float,
) -> Optional[BatchCost]:
    """Calculate cost for a single batch from usage info and pricing.

    Returns BatchCost or None if usage dict is missing or lacks required keys.
    """
    if not isinstance(usage, dict):
        return None

    try:
        prompt_tokens = usage["prompt_tokens"]
        completion_tokens = usage["completion_tokens"]
    except KeyError:
        return None

    cost_usd = prompt_tokens * prompt_price + completion_tokens * completion_price
    return BatchCost(
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        cost_usd=cost_usd,
    )


def accumulate_cost(session: SessionCost, batch: BatchCost) -> SessionCost:
    """Accumulate batch cost into session totals."""
    return SessionCost(
        total_prompt_tokens=session.total_prompt_tokens + batch.prompt_tokens,
        total_completion_tokens=session.total_completion_tokens + batch.completion_tokens,
        total_cost_usd=session.total_cost_usd + batch.cost_usd,
    )
