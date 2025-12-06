from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import json
import logging
import os
from math import isfinite

try:
    # OpenAI v1.x client
    from openai import OpenAI, BadRequestError  # type: ignore
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore
    BadRequestError = Exception  # type: ignore

from .ai_models import get_model_whitelist, normalize_model_choice


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
        self._model = normalize_model_choice(model, log_prefix="AIClient")
        self._enabled = bool(self._api_key and OpenAI is not None)
        self._token_param = self._detect_token_param(self._model)
        # Configure timeout: default 30 seconds, configurable via OPENAI_TIMEOUT env var
        timeout_seconds = float(os.getenv("OPENAI_TIMEOUT", "30.0"))
        self._timeout = timeout_seconds
        self._client = OpenAI(api_key=self._api_key, timeout=timeout_seconds) if self._enabled else None

    @staticmethod
    def _detect_token_param(model_name: str) -> str:
        """Newer JSON endpoints expect max_completion_tokens instead of max_tokens."""
        prefixes = ("gpt-5", "gpt-4.1", "o1", "o3", "o4")
        if any(model_name.startswith(p) for p in prefixes):
            return "max_completion_tokens"
        return "max_tokens"

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
        temperature: float | None = 0.2,
        max_output_tokens: int | None = None,
        extra_messages: Optional[list[dict[str, Any]]] = None,
        include_raw: bool = False,
        include_request_payload: bool = False,
        image_urls: Optional[List[str]] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
        tool_choice: Optional[Union[str, Dict[str, Any]]] = None,
        tool_handlers: Optional[Dict[str, Callable[[Dict[str, Any]], str]]] = None,
    ) -> Union[Dict[str, Any], Tuple[Dict[str, Any], str], Tuple[Dict[str, Any], Dict[str, Any]], Tuple[Dict[str, Any], str, Dict[str, Any]]]:
        if not self._enabled or not self._client:
            raise RuntimeError("AI client is not configured. Set OPENAI_API_KEY.")
        
        # Ensure we always return a dict, never None
        try:

        messages: list[dict[str, Any]] = []
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
                if not (isinstance(m, dict) and "role" in m and "content" in m):
                    continue
                role = str(m["role"])
                content = m["content"]
                if isinstance(content, (list, dict)):
                    messages.append({"role": role, "content": content})
                else:
                    messages.append({"role": role, "content": str(content or "")})
        if image_urls:
            parts: list[dict[str, Any]] = []
            if user_prompt:
                parts.append({"type": "text", "text": user_prompt})
            for img in image_urls:
                if not img:
                    continue
                parts.append({"type": "image_url", "image_url": {"url": str(img)}})
            if parts:
                messages.append({"role": "user", "content": parts})
            else:
                messages.append({"role": "user", "content": user_prompt})
        else:
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

        def _build_kwargs(current_messages: list[dict[str, Any]]) -> Dict[str, Any]:
            payload: Dict[str, Any] = {
                "model": self._model,
                "messages": current_messages,
            }
            # CRITICAL: response_format and tools are mutually exclusive in OpenAI API
            # Only use JSON mode when tools are NOT provided
            if not tools:
                payload["response_format"] = {"type": "json_object"}
            if temperature is not None:
                payload["temperature"] = temperature
            payload[self._token_param] = max_output_tokens
            if tools:
                payload["tools"] = tools
                if tool_choice is not None:
                    payload["tool_choice"] = tool_choice
            return payload

        def _run_completion(current_messages: list[dict[str, Any]]) -> Any:
            completion_kwargs = _build_kwargs(current_messages)
            try:
                return self._client.chat.completions.create(**completion_kwargs)
            except BadRequestError as exc:
                msg = str(exc).lower()
                if "temperature" in msg and "unsupported" in msg and "1" in msg:
                    if "temperature" in completion_kwargs:
                        logging.getLogger("ai").warning(
                            "Model %s rejected temperature=%s; retrying with provider default",
                            self._model,
                            completion_kwargs.get("temperature"),
                        )
                        completion_kwargs.pop("temperature", None)
                        return self._client.chat.completions.create(**completion_kwargs)
                if "max_completion_tokens" in msg and self._token_param == "max_tokens":
                    completion_kwargs.pop("max_tokens", None)
                    completion_kwargs["max_completion_tokens"] = max_output_tokens
                    self._token_param = "max_completion_tokens"
                    return self._client.chat.completions.create(**completion_kwargs)
                raise

        response = _run_completion(messages)
        tool_loop_count = 0
        max_tool_loops = int(os.getenv("AI_MAX_TOOL_CALLS", "3"))
        while getattr(response.choices[0].message, "tool_calls", None):
            if not tool_handlers:
                raise RuntimeError("Model requested a tool call but no handlers were provided.")
            if tool_loop_count >= max_tool_loops:
                raise RuntimeError("Exceeded maximum tool call iterations.")
            tool_loop_count += 1
            choice = response.choices[0]
            assistant_entry: Dict[str, Any] = {
                "role": "assistant",
                "content": choice.message.content or "",
                "tool_calls": [],
            }
            for tool_call in choice.message.tool_calls or []:
                assistant_entry["tool_calls"].append(
                    {
                        "id": tool_call.id,
                        "type": tool_call.type,
                        "function": {
                            "name": tool_call.function.name,
                            "arguments": tool_call.function.arguments,
                        },
                    }
                )
            messages.append(assistant_entry)
            for tool_call in choice.message.tool_calls or []:
                handler = tool_handlers.get(tool_call.function.name)
                if handler is None:
                    raise RuntimeError(f"No handler registered for tool {tool_call.function.name}")
                try:
                    args = json.loads(tool_call.function.arguments or "{}")
                except Exception:
                    args = {}
                try:
                    tool_output = handler(args) or ""
                except Exception as handler_exc:
                    tool_output = json.dumps({"error": str(handler_exc)}, ensure_ascii=False)
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": tool_output,
                    }
                )
            response = _run_completion(messages)
        
        # Capture final request payload if requested
        final_request_payload = None
        if include_request_payload:
            final_request_payload = _build_kwargs(messages)

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
                    if include_request_payload and final_request_payload:
                        if include_raw:
                            return data, txt, final_request_payload
                        else:
                            return data, final_request_payload
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
            if include_request_payload and final_request_payload:
                if include_raw:
                    return data, txt, final_request_payload
                else:
                    return data, final_request_payload
            if include_raw:
                return data, txt
            return data


    def generate_chat(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float | None = 0.2,
        max_output_tokens: int | None = None,
        extra_messages: Optional[list[dict[str, Any]]] = None,
        include_raw: bool = False,
        include_request_payload: bool = False,
        image_urls: Optional[List[str]] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
        tool_choice: Optional[Union[str, Dict[str, Any]]] = None,
        tool_handlers: Optional[Dict[str, Callable[[Dict[str, Any]], str]]] = None,
        response_format: Optional[Dict[str, Any]] = None,
    ) -> Union[str, Tuple[str, str], Tuple[str, Dict[str, Any]], Tuple[str, str, Dict[str, Any]]]:
        """
        General chat generation with optional tool calls (no JSON parsing).
        """
        if not self._enabled or not self._client:
            raise RuntimeError("AI client is not configured. Set OPENAI_API_KEY.")

        messages: list[dict[str, Any]] = []
        if system_prompt:
            messages.append({"role": "system", "content": str(system_prompt)})
        if extra_messages:
            for m in extra_messages:
                if not (isinstance(m, dict) and "role" in m and "content" in m):
                    continue
                role = str(m["role"])
                content = m["content"]
                if isinstance(content, (list, dict)):
                    messages.append({"role": role, "content": content})
                else:
                    messages.append({"role": role, "content": str(content or "")})
        if image_urls:
            parts: list[dict[str, Any]] = []
            if user_prompt:
                parts.append({"type": "text", "text": user_prompt})
            for img in image_urls:
                if not img:
                    continue
                parts.append({"type": "image_url", "image_url": {"url": str(img)}})
            if parts:
                messages.append({"role": "user", "content": parts})
            else:
                messages.append({"role": "user", "content": user_prompt})
        else:
            messages.append({"role": "user", "content": user_prompt})

        if max_output_tokens is None:
            ctx_limit = int(os.getenv("AI_CTX_LIMIT", "128000"))
            out_ratio = float(os.getenv("AI_TARGET_OUT_RATIO", "2"))
            safety_in = int(os.getenv("AI_SAFETY_IN_TOK", "512"))
            safety_out_min = int(os.getenv("AI_SAFETY_OUT_MIN", "1024"))

            joined = "".join([(m.get("content") or "") for m in messages])
            in_tokens = _estimate_tokens(joined)
            target_out = int(in_tokens * out_ratio) + 1024
            legacy_floor = 10000
            desired = max(target_out, legacy_floor)
            available = max(safety_out_min, ctx_limit - in_tokens - safety_in)
            max_output_tokens = max(1, min(desired, available))
        else:
            in_tokens = _estimate_tokens("".join([(m.get("content") or "") for m in messages]))

        def _build_kwargs(current_messages: list[dict[str, Any]]) -> Dict[str, Any]:
            payload: Dict[str, Any] = {
                "model": self._model,
                "messages": current_messages,
            }
            if response_format:
                payload["response_format"] = response_format
            if temperature is not None:
                payload["temperature"] = temperature
            payload[self._token_param] = max_output_tokens
            if tools:
                payload["tools"] = tools
                if tool_choice is not None:
                    payload["tool_choice"] = tool_choice
            return payload

        def _run_completion(current_messages: list[dict[str, Any]]) -> Any:
            completion_kwargs = _build_kwargs(current_messages)
            try:
                return self._client.chat.completions.create(**completion_kwargs)
            except BadRequestError as exc:
                msg = str(exc).lower()
                if "temperature" in msg and "unsupported" in msg and "1" in msg:
                    if "temperature" in completion_kwargs:
                        logging.getLogger("ai").warning(
                            "Model %s rejected temperature=%s; retrying with provider default",
                            self._model,
                            completion_kwargs.get("temperature"),
                        )
                        completion_kwargs.pop("temperature", None)
                        return self._client.chat.completions.create(**completion_kwargs)
                if "max_completion_tokens" in msg and self._token_param == "max_tokens":
                    completion_kwargs.pop("max_tokens", None)
                    completion_kwargs["max_completion_tokens"] = max_output_tokens
                    self._token_param = "max_completion_tokens"
                    return self._client.chat.completions.create(**completion_kwargs)
                raise

        response = _run_completion(messages)
        tool_loop_count = 0
        max_tool_loops = int(os.getenv("AI_MAX_TOOL_CALLS", "3"))
        while getattr(response.choices[0].message, "tool_calls", None):
            if not tool_handlers:
                raise RuntimeError("Model requested a tool call but no handlers were provided.")
            if tool_loop_count >= max_tool_loops:
                raise RuntimeError("Exceeded maximum tool call iterations.")
            tool_loop_count += 1
            choice = response.choices[0]
            assistant_entry: Dict[str, Any] = {
                "role": "assistant",
                "content": choice.message.content or "",
                "tool_calls": [],
            }
            for tool_call in choice.message.tool_calls or []:
                assistant_entry["tool_calls"].append(
                    {
                        "id": tool_call.id,
                        "type": tool_call.type,
                        "function": {
                            "name": tool_call.function.name,
                            "arguments": tool_call.function.arguments,
                        },
                    }
                )
            messages.append(assistant_entry)
            for tool_call in choice.message.tool_calls or []:
                handler = tool_handlers.get(tool_call.function.name) if tool_handlers else None
                if handler is None:
                    raise RuntimeError(f"No handler registered for tool {tool_call.function.name}")
                try:
                    args = json.loads(tool_call.function.arguments or "{}")
                except Exception:
                    args = {}
                try:
                    tool_output = handler(args) or ""
                except Exception as handler_exc:
                    tool_output = json.dumps({"error": str(handler_exc)}, ensure_ascii=False)
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": tool_output,
                    }
                )
            response = _run_completion(messages)

        final_request_payload = None
        if include_request_payload:
            final_request_payload = _build_kwargs(messages)

        txt = (response.choices[0].message.content or "").strip()
        try:
            print("[AI DEBUG] in_tokens=", in_tokens, "max_tokens=", max_output_tokens, "raw_len=", len(txt))
        except Exception:
            pass

        if include_request_payload and final_request_payload:
            if include_raw:
                return txt, txt, final_request_payload
            return txt, final_request_payload
        if include_raw:
            return txt, txt
        return txt


def get_ai_shadow_model_from_settings(default: str = "gpt-4o-mini") -> str:
    """Get shadow AI model from DB first, falling back to env var and default."""
    try:
        from ..db import get_session
        from ..models import SystemSetting
        from sqlmodel import select

        with get_session() as session:
            setting = session.exec(
                select(SystemSetting).where(SystemSetting.key == "ai_shadow_model")
            ).first()
            if setting and setting.value:
                return normalize_model_choice(setting.value, default=default, log_prefix="AI shadow DB")
    except Exception:
        logging.getLogger("ai_shadow").warning("Shadow model DB lookup failed", exc_info=True)

    env_model = os.getenv("AI_SHADOW_MODEL")
    if env_model:
        return normalize_model_choice(env_model, default=default, log_prefix="AI shadow env")

    return normalize_model_choice(default, log_prefix="AI shadow default")


def get_shadow_temperature_setting(default: float = 0.1) -> float:
    """Return temperature (0-2 range) for shadow replies, preferring DB over env."""

    def _sanitize(value: float | str | None, fallback: float) -> float:
        try:
            temp = float(value) if value is not None else fallback
            if not isfinite(temp):
                raise ValueError("non-finite")
        except Exception:
            return fallback
        return max(0.0, min(temp, 2.0))

    try:
        from ..db import get_session
        from ..models import SystemSetting
        from sqlmodel import select

        with get_session() as session:
            setting = session.exec(
                select(SystemSetting).where(SystemSetting.key == "ai_shadow_temperature")
            ).first()
            if setting and setting.value:
                default = _sanitize(setting.value, default)
    except Exception:
        logging.getLogger("ai_shadow").warning("Shadow temperature DB lookup failed", exc_info=True)

    env_temp = os.getenv("AI_REPLY_TEMPERATURE")
    if env_temp:
        return _sanitize(env_temp, default)
    return _sanitize(default, default)


def is_shadow_temperature_opt_out() -> bool:
    """Whether we should avoid sending a temperature param altogether."""
    def _as_bool(val: str | bool | None, fallback: bool = False) -> bool:
        if isinstance(val, bool):
            return val
        if val is None:
            return fallback
        return str(val).strip().lower() in ("1", "true", "yes", "on")

    try:
        from ..db import get_session
        from ..models import SystemSetting
        from sqlmodel import select

        with get_session() as session:
            setting = session.exec(
                select(SystemSetting).where(SystemSetting.key == "ai_shadow_temperature_opt_out")
            ).first()
            if setting and setting.value is not None:
                return _as_bool(setting.value)
    except Exception:
        logging.getLogger("ai_shadow").warning("Shadow temperature opt-out lookup failed", exc_info=True)

    env_val = os.getenv("AI_SHADOW_TEMPERATURE_OPT_OUT")
    if env_val is not None:
        return _as_bool(env_val)
    return False


def get_ai_model_from_settings(default: str = "gpt-4o-mini") -> str:
    """Get the AI model from system settings.
    
    Args:
        default: Default model to use if setting is not found
        
    Returns:
        The model name from settings or default
    """
    try:
        from ..db import get_session
        from ..models import SystemSetting
        from sqlmodel import select
        
        with get_session() as session:
            setting = session.exec(
                select(SystemSetting).where(SystemSetting.key == "ai_model")
            ).first()
            
            if setting and setting.value:
                return normalize_model_choice(setting.value, default=default, log_prefix="AI model DB")
            return normalize_model_choice(default, log_prefix="AI model default")
    except Exception:
        # If there's any error, return default
        return default


