"""Translations of the bot's chat messages.

Every message has a key; its text per language lives in TOML files:

    locales/<lang>.toml          shipped with the bot (en is complete)
    locales/custom/<lang>.toml   your own: a new language, or overrides of some
                                 keys of a shipped one (en/ru ship with every
                                 key commented out, ready to edit)

A text is a string with {named} placeholders (str.format syntax, so
"{seconds:.1f}" works), or a table of plural forms chosen by the `count`
placeholder:

    tracks = { one = "{count} track", other = "{count} tracks" }
    tracks = { one = "{count} трек", few = "{count} трека", many = "{count} треков" }

Anything missing or broken in a translation (unknown key, a placeholder the
code doesn't pass) falls back to English with a warning in the log, so a
custom file can never make the bot fail.
"""
import logging
import string
import tomllib
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_LANGUAGE = "en"
_FORMATTER = string.Formatter()


def plural_category(language: str, count: int) -> str:
    """CLDR plural category for the languages we know; "other" for the rest."""
    n = abs(int(count))
    if language in {"ru", "uk", "be"}:
        if n % 10 == 1 and n % 100 != 11:
            return "one"
        if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
            return "few"
        return "many"
    return "one" if n == 1 else "other"


def _placeholders(text: str) -> set[str]:
    names = set()
    for _, field, _, _ in _FORMATTER.parse(text):
        if field:
            names.add(field.split(".")[0].split("[")[0])
    return names


class Translations:
    def __init__(self, builtin_dir: Path, custom_dir: Path | None = None):
        self.builtin_dir = builtin_dir
        self.custom_dir = custom_dir
        self.catalogs: dict[str, dict[str, Any]] = {}
        self.names: dict[str, str] = {}
        self.reload()

    def reload(self):
        catalogs: dict[str, dict[str, Any]] = {}
        for directory in (self.builtin_dir, self.custom_dir):
            if not directory or not directory.is_dir():
                continue
            for path in sorted(directory.glob("*.toml")):
                try:
                    data = tomllib.loads(path.read_text(encoding="utf-8"))
                except (OSError, tomllib.TOMLDecodeError) as exc:
                    logger.warning("Locale %s skipped: %s", path, exc)
                    continue
                catalogs.setdefault(path.stem, {}).update(data)
        if DEFAULT_LANGUAGE not in catalogs:
            raise RuntimeError(f"{self.builtin_dir}/{DEFAULT_LANGUAGE}.toml is missing")
        self.catalogs = catalogs
        self.names = {lang: str(cat.get("language_name", lang)) for lang, cat in catalogs.items()}
        self._check()

    def _check(self):
        """Warn once at load about keys whose placeholders differ from English."""
        base = self.catalogs[DEFAULT_LANGUAGE]
        for lang, catalog in self.catalogs.items():
            if lang == DEFAULT_LANGUAGE:
                continue
            for key, value in catalog.items():
                if key == "language_name":
                    continue
                if key not in base:
                    logger.warning("Locale %s: unknown key %r (ignored)", lang, key)
                    continue
                expected = self._names_of(base[key])
                for form in (value.values() if isinstance(value, dict) else [value]):
                    extra = _placeholders(str(form)) - expected
                    if extra:
                        logger.warning("Locale %s: %r uses unknown placeholders %s (English is used)",
                                       lang, key, sorted(extra))

    @staticmethod
    def _names_of(value: Any) -> set[str]:
        forms = value.values() if isinstance(value, dict) else [value]
        names: set[str] = set()
        for form in forms:
            names |= _placeholders(str(form))
        return names | {"count"}

    @property
    def languages(self) -> list[str]:
        return sorted(self.catalogs)

    def text(self, language: str, key: str, /, **values: Any) -> str:
        for lang in (language, DEFAULT_LANGUAGE):
            catalog = self.catalogs.get(lang)
            if not catalog or key not in catalog:
                continue
            entry = catalog[key]
            try:
                if isinstance(entry, dict):
                    category = plural_category(lang, values.get("count", 0))
                    entry = entry.get(category) or entry.get("other") or entry.get("many") or next(iter(entry.values()))
                return str(entry).format(**values)
            except (KeyError, IndexError, ValueError, AttributeError) as exc:
                logger.warning("Locale %s: %r failed to format (%r), using English", lang, key, exc)
        logger.warning("Message key %r has no English text", key)
        return key
