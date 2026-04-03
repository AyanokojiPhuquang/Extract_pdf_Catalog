"""Model cache module — caches vision-capable AI models from OpenRouter API."""

import logging
import time
from dataclasses import dataclass
from typing import Optional

import requests

log = logging.getLogger("extractor")

CACHE_TTL = 3600  # 1 hour


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
            architecture = item.get("architecture") or {}
            modality = architecture.get("modality", "")
            if "image" not in modality:
                continue

            pricing = item.get("pricing") or {}
            models.append(
                ModelInfo(
                    id=item["id"],
                    name=item.get("name", item["id"]),
                    prompt_price=float(pricing.get("prompt", 0)),
                    completion_price=float(pricing.get("completion", 0)),
                )
            )

        log.info(f"Found {len(models)} vision-capable models")
        return models
