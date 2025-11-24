from __future__ import annotations

import json
import logging
import os
from collections import OrderedDict
from typing import Dict, List, Tuple

from openai import OpenAI

from ..db import get_session
from ..models import SystemSetting

DEFAULT_MODEL = "gpt-4o-mini"

# Fallback list used when there is no data in DB yet
DEFAULT_MODEL_WHITELIST: List[str] = [
    "gpt-5-mini",
    "gpt-5-pro",
    "gpt-5",
    "gpt-4o-mini",
    "gpt-4o",
    "gpt-4-turbo",
    "gpt-4",
    "gpt-3.5-turbo",
    "o1-preview",
    "o1-mini",
]

SETTING_KEY = "ai_model_whitelist"


def _read_setting() -> List[str]:
    with get_session() as session:
        setting = session.get(SystemSetting, SETTING_KEY)
        if not setting or not (setting.value or "").strip():
            return []
        try:
            data = json.loads(setting.value)
            if isinstance(data, list):
                return [str(x) for x in data if isinstance(x, str)]
        except Exception:
            logging.warning("ai_model_whitelist setting is not valid JSON, falling back to defaults")
    return []


def _write_setting(models: List[str]) -> None:
    payload = json.dumps(models, ensure_ascii=False)
    with get_session() as session:
        setting = session.get(SystemSetting, SETTING_KEY)
        if not setting:
            setting = SystemSetting(key=SETTING_KEY, value=payload, description="Cached OpenAI model whitelist")
        else:
            setting.value = payload
        session.add(setting)
        session.commit()


def get_model_whitelist() -> List[str]:
    stored = _read_setting()
    if stored:
        return stored
    # seed DB with default list so future calls are fast
    _write_setting(sorted(set(DEFAULT_MODEL_WHITELIST), key=str.lower))
    return DEFAULT_MODEL_WHITELIST[:]


def normalize_model_choice(model_name: str | None, default: str | None = None, *, log_prefix: str | None = None) -> str:
    whitelist = get_model_whitelist()
    if model_name and model_name in whitelist:
        return model_name
    if model_name and log_prefix:
        logging.warning("%s falling back to default model because '%s' is not in whitelist", log_prefix, model_name)
    if whitelist:
        return whitelist[0]
    return default or DEFAULT_MODEL


def refresh_openai_model_whitelist() -> Dict[str, List[str]]:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    response = client.models.list()
    fetched: List[str] = []
    for model in response.data:
        mid = getattr(model, "id", None)
        if not isinstance(mid, str):
            continue
        if _is_supported_model_id(mid):
            fetched.append(mid)
    cleaned = sorted(set(fetched), key=str.lower)
    if not cleaned:
        raise RuntimeError("OpenAI model list did not return any supported GPT/o1 models")

    current = set(get_model_whitelist())
    new_set = set(cleaned)
    added = sorted(new_set - current, key=str.lower)
    removed = sorted(current - new_set, key=str.lower)
    _write_setting(cleaned)
    return {"added": added, "removed": removed, "all": cleaned}


def _is_supported_model_id(model_id: str) -> bool:
    prefixes = ("gpt-", "o1", "omni", "chatgpt", "text-", "ft:")
    return model_id.startswith(prefixes)


def group_model_names(models: List[str]) -> Dict[str, List[str]]:
    groups: OrderedDict[str, List[str]] = OrderedDict()

    def _label(name: str) -> str:
        if name.startswith("gpt-5"):
            return "GPT-5"
        if name.startswith("gpt-4o"):
            return "GPT-4o"
        if name.startswith("gpt-4"):
            return "GPT-4"
        if name.startswith("gpt-3.5"):
            return "GPT-3.5"
        if name.startswith("o1"):
            return "o1 / Reasoning"
        return "Other"

    for model in models:
        label = _label(model)
        groups.setdefault(label, []).append(model)

    return groups

