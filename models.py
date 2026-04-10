"""Model cache module — caches vision-capable AI models from OpenRouter API."""

import logging
import time
from dataclasses import dataclass
from typing import Optional

import requests

log = logging.getLogger("extractor")

CACHE_TTL = 3600  # 1 hour

# Top vision models for PDF/catalogue extraction
TOP_MODEL_IDS = {
    "anthropic/claude-sonnet-4",
    "anthropic/claude-opus-4",
    "google/gemini-2.5-pro-preview",
    "google/gemini-2.5-flash-preview",
    "google/gemini-2.0-flash-001",
    "openai/gpt-4.1",
    "openai/gpt-4.1-mini",
    "openai/gpt-4o",
    "openai/o4-mini",
    "meta-llama/llama-4-maverick",
    "google/gemma-3-27b-it:free",
    "google/gemma-3n-e4b-it:free",
    "meta-llama/llama-3.2-11b-vision-instruct:free",
}


@dataclass
class ModelInfo:
    id: str
    name: str
    prompt_price: float  # USD per token
    completion_price: float  # USD per token


class ModelCache:
    def __init__(self) -> None:
        self._models: list[ModelInfo] = []
        self._last_fetched: float = 0

    def get_models(self, api_key: str) -> list[ModelInfo]:
        """Return list of vision-capable models. Fetches from API if cache expired."""
        if time.time() - self._last_fetched >= CACHE_TTL:
            self._models = self._fetch_models(api_key)
            self._last_fetched = time.time()
        return list(self._models)

    def get_model_pricing(self, model_id: str) -> Optional[ModelInfo]:
        """Return pricing info for a specific model from cache."""
        for model in self._models:
            if model.id == model_id:
                return model
        return None

    def _fetch_models(self, api_key: str) -> list[ModelInfo]:
        """Call OpenRouter API GET /api/v1/models, filter vision models."""
        log.info("Fetching model list from OpenRouter API...")
        resp = requests.get(
            "https://openrouter.ai/api/v1/models",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        models: list[ModelInfo] = []
        for item in data.get("data", []):
            model_id = item["id"]
            # Include if in our curated list OR has vision capability
            architecture = item.get("architecture") or {}
            modality = architecture.get("modality", "")
            if model_id not in TOP_MODEL_IDS and "image" not in modality:
                continue

            pricing = item.get("pricing") or {}
            models.append(
                ModelInfo(
                    id=model_id,
                    name=item.get("name", model_id),
                    prompt_price=float(pricing.get("prompt", 0)),
                    completion_price=float(pricing.get("completion", 0)),
                )
            )

        log.info(f"Found {len(models)} vision-capable models")
        # Filter to top models only
        top = [m for m in models if m.id in TOP_MODEL_IDS]
        # Sort: keep a consistent order based on TOP_MODEL_IDS list
        order = {mid: i for i, mid in enumerate(TOP_MODEL_IDS)}
        top.sort(key=lambda m: order.get(m.id, 999))
        log.info(f"Returning {len(top)} curated models")
        return top
