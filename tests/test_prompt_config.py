import importlib.util
from pathlib import Path

import pytest


def _load_example_config():
    path = Path(__file__).resolve().parents[1] / "app" / "config.example.py"
    spec = importlib.util.spec_from_file_location("config_example_for_test", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load config.example.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_prompt_keys_match_between_config_and_example():
    config_path = Path(__file__).resolve().parents[1] / "app" / "config.py"
    if not config_path.exists():
        pytest.skip("local app/config.py is not present")

    from app import config

    example_config = _load_example_config()

    assert set(config.PROMPTS) == set(example_config.PROMPTS)
    for key, translations in config.PROMPTS.items():
        assert set(translations) == {"zh", "en"}
        assert set(example_config.PROMPTS[key]) == {"zh", "en"}
        assert translations["zh"].strip()
        assert translations["en"].strip()
        assert example_config.PROMPTS[key]["zh"].strip()
        assert example_config.PROMPTS[key]["en"].strip()

