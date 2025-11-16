from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, Union
import json
import os

try:
    # OpenAI v1.x client
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


def _estimate_tokens(text: str) -> int:
    """Rough token estimator.

    Uses tiktoken if available; otherwise ~4 chars per token heuristic.
    """
    try:
        import tiktoken  # type: ignore
        enc = tiktoken.get_encoding("cl100k_base")
        return len(enc.encode(text or ""))
    except Exception:
        return max(1, len(text or "") // 4)


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

    @property
    def model(self) -> str:
        return self._model

    def generate_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
        max_output_tokens: int | None = None,
        extra_messages: Optional[list[dict[str, Any]]] = None,
        include_raw: bool = False,
    ) -> Union[Dict[str, Any], Tuple[Dict[str, Any], str]]:
        if not self._enabled or not self._client:
            raise RuntimeError("AI client is not configured. Set OPENAI_API_KEY.")

        messages: list[dict[str, str]] = []
        if system_prompt:
            # Ensure the system prompt contains the word "json" so OpenAI allows JSON mode
            sys_txt = str(system_prompt)
            if "json" not in sys_txt.lower():
                sys_txt = (sys_txt + "\n\n" + "You MUST respond with a single JSON object.").strip()
            messages.append({"role": "system", "content": sys_txt})
        else:
            # Default system message for JSON mode
            messages.append(
                {
                    "role": "system",
                    "content": "You are a strict JSON generator. You MUST respond with a single valid JSON object.",
                }
            )
        if extra_messages:
            for m in extra_messages:
                # minimal validation to avoid type errors
                if isinstance(m, dict) and "role" in m and "content" in m:
                    messages.append({"role": str(m["role"]), "content": str(m["content"])})
        messages.append({"role": "user", "content": user_prompt})

        # Compute dynamic max tokens if not provided
        if max_output_tokens is None:
            # Defaults target for 128k context models; can be overridden via env
            ctx_limit = int(os.getenv("AI_CTX_LIMIT", "128000"))
            out_ratio = float(os.getenv("AI_TARGET_OUT_RATIO", "2"))
            safety_in = int(os.getenv("AI_SAFETY_IN_TOK", "512"))
            safety_out_min = int(os.getenv("AI_SAFETY_OUT_MIN", "1024"))

            joined = "".join([(m.get("content") or "") for m in messages])
            in_tokens = _estimate_tokens(joined)
            target_out = int(in_tokens * out_ratio) + 1024
            # If computed target is smaller than legacy default (2000), behave like before
            legacy_floor = 10000
            desired = max(target_out, legacy_floor)
            available = max(safety_out_min, ctx_limit - in_tokens - safety_in)
            max_output_tokens = max(1, min(desired, available))

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
            print("[AI DEBUG] in_tokens=", in_tokens, "max_tokens=", max_output_tokens, "raw_len=", len(txt))
        except Exception:
            pass
        try:
            data = json.loads(txt)
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
                    data = json.loads(segment)
                    if include_raw:
                        return data, txt
                    return data
            except Exception:
                pass
            # Final fallback: return empty, with warnings including raw text (truncated)
            raw_full = cleaned if 'cleaned' in locals() else txt
            data = {
                "products_to_create": [],
                "mappings_to_create": [],
                "notes": None,
                "warnings": [
                    "AI yanıtı geçerli JSON değil; öneriler boş döndü.",
                    f"AI raw: {raw_full}"
                ],
            }
            if include_raw:
                return data, txt
            return data
        if include_raw:
            return data, txt
        return data


