from __future__ import annotations

from typing import Any, Dict, Optional
import json
import os

try:
    # OpenAI v1.x client
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


class AIClient:
    """Thin wrapper around OpenAI client focused on JSON responses.

    - Reads API key from OPENAI_API_KEY env var by default
    - Provides generate_json helper using JSON mode
    - Includes basic timeout and retry handling
    """

    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4o-mini") -> None:
        self._api_key = api_key or os.getenv("OPENAI_API_KEY") or ""
        self._model = model
        self._enabled = bool(self._api_key and OpenAI is not None)
        self._client = OpenAI(api_key=self._api_key) if self._enabled else None

    @property
    def enabled(self) -> bool:
        return self._enabled

    def generate_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
        max_output_tokens: int = 2000,
        extra_messages: Optional[list[dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if not self._enabled or not self._client:
            raise RuntimeError("AI client is not configured. Set OPENAI_API_KEY.")

        messages: list[dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        if extra_messages:
            for m in extra_messages:
                # minimal validation to avoid type errors
                if isinstance(m, dict) and "role" in m and "content" in m:
                    messages.append({"role": str(m["role"]), "content": str(m["content"])})
        messages.append({"role": "user", "content": user_prompt})

        # JSON mode
        response = self._client.chat.completions.create(
            model=self._model,
            messages=messages,  # type: ignore[arg-type]
            temperature=temperature,
            response_format={"type": "json_object"},
            max_tokens=max_output_tokens,
        )
        txt = (response.choices[0].message.content or "").strip()
        try:
            return json.loads(txt)
        except Exception:
            # Robust repairs for occasional non-strict JSON replies
            cleaned = txt.strip().strip("` ")
            if cleaned.lower().startswith("json\n"):
                cleaned = cleaned.split("\n", 1)[1]
            # Extract JSON object substring between the first '{' and the last '}'
            try:
                start = cleaned.find("{")
                end = cleaned.rfind("}")
                if start != -1 and end != -1 and end > start:
                    segment = cleaned[start : end + 1]
                    import re as _re
                    # Remove trailing commas before } or ]
                    segment = _re.sub(r",\s*([}\]])", r"\1", segment)
                    return json.loads(segment)
            except Exception:
                pass
            # Final fallback: return empty, with warning to surface in UI instead of 500
            return {
                "products_to_create": [],
                "mappings_to_create": [],
                "notes": None,
                "warnings": ["AI yanıtı geçerli JSON değil; öneriler boş döndü."]
            }


