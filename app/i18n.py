import json
import os
from contextvars import ContextVar
from pathlib import Path
from typing import Any

# Global context for current language
_lang: ContextVar[str] = ContextVar("lang", default="zh")

LANG_DIR = Path(__file__).parent / "lang"
_translations: dict[str, dict[str, str]] = {}

def load_translations():
    global _translations
    for lang in ["zh", "en"]:
        file_path = LANG_DIR / f"{lang}.json"
        if file_path.exists():
            with open(file_path, "r", encoding="utf-8") as f:
                _translations[lang] = json.load(f)
        else:
            _translations[lang] = {}

# Initialize on import
load_translations()

def set_lang(lang: str):
    """Set the language for the current context."""
    if lang not in ["zh", "en"]:
        lang = "zh"
    _lang.set(lang)

def get_lang() -> str:
    """Get the current context language."""
    return _lang.get()

def t(key: str, **kwargs: Any) -> str:
    """Translate a key using the current context language."""
    lang = get_lang()
    msg = _translations.get(lang, {}).get(key, _translations.get("zh", {}).get(key, key))
    if kwargs:
        return msg.format(**kwargs)
    return msg
