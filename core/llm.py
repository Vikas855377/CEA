from __future__ import annotations

import json
from typing import Any

from core.config import OpenAISettings


class LLMClient:
    def __init__(self, settings: OpenAISettings | None = None) -> None:
        self.settings = settings or OpenAISettings.from_env()

    @property
    def is_configured(self) -> bool:
        if self.settings.provider == "azure":
            return bool(
                self.settings.azure_api_key
                and self.settings.azure_endpoint
                and self.settings.azure_deployment
            )
        return bool(self.settings.api_key)

    def analyze_json(self, *, system_prompt: str, user_payload: dict[str, Any]) -> dict[str, Any]:
        if not self.is_configured:
            raise RuntimeError("OpenAI settings are incomplete. Fill OpenAI or Azure OpenAI values in .env.")

        client = self._client()
        model = self.settings.azure_deployment if self.settings.provider == "azure" else self.settings.model
        response = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_payload, default=str)},
            ],
            text={"format": {"type": "json_object"}},
        )
        return json.loads(response.output_text)

    def _client(self) -> Any:
        if self.settings.provider == "azure":
            from openai import AzureOpenAI

            return AzureOpenAI(
                api_key=self.settings.azure_api_key,
                azure_endpoint=self.settings.azure_endpoint,
                api_version=self.settings.azure_api_version,
            )

        from openai import OpenAI

        return OpenAI(api_key=self.settings.api_key)
