"""Turn Spotify links into YouTube search queries.

Spotify audio is DRM-protected and yt-dlp has no extractor for it, so the bot
only reads the metadata (title + artists) and lets yt-dlp search YouTube.

Metadata comes from the public embed page (open.spotify.com/embed/<kind>/<id>),
which carries a __NEXT_DATA__ JSON blob and needs no API key. It lists up to
the first 100 tracks of a playlist/album, or an artist's top tracks.
"""

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

import aiohttp


logger = logging.getLogger(__name__)

SUPPORTED_KINDS = {"track", "album", "playlist", "artist"}
UNSUPPORTED_KINDS = {"episode", "show", "audiobook", "chapter"}

_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0"
_ID_RE = re.compile(r"^[A-Za-z0-9]{22}$")
_URI_RE = re.compile(r"^spotify:([a-z]+):([A-Za-z0-9]{22})$")
_OPEN_URL_IN_TEXT_RE = re.compile(r"https://open\.spotify\.com/(?:intl-[a-zA-Z-]+/)?([a-z]+)/([A-Za-z0-9]{22})")
_NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', re.S)
# "Dreams - 2004 Remaster", "Help! - Remastered 2009": Spotify-only suffixes
# that YouTube uploads usually lack, so they only hurt the search.
_REMASTER_SUFFIX_RE = re.compile(r"\s+-\s+[^-]*\bremaster(?:ed)?\b[^-]*$", re.IGNORECASE)
_SHORT_HOSTS = {"spotify.link", "spoti.fi", "spotify.app.link"}
_TIMEOUT = aiohttp.ClientTimeout(total=15)


class SpotifyError(Exception):
    """User-facing error while resolving a Spotify link."""


@dataclass
class SpotifyTrack:
    title: str
    artists: list[str]

    @property
    def query(self) -> str:
        # The first artist + title matches the YouTube upload far more often
        # than a long "A, B, C - Title" string.
        title = _REMASTER_SUFFIX_RE.sub("", self.title) or self.title
        if self.artists:
            return f"{self.artists[0]} - {title}"
        return title

    @property
    def display(self) -> str:
        if self.artists:
            return f"{', '.join(self.artists)} — {self.title}"
        return self.title


@dataclass
class SpotifyItem:
    kind: str
    name: str
    tracks: list[SpotifyTrack] = field(default_factory=list)


def is_spotify_link(value: str) -> bool:
    raw = (value or "").strip()
    if raw.startswith("spotify:"):
        return True
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    try:
        host = (urlparse(raw).hostname or "").lower()
    except ValueError:
        return False
    return host == "open.spotify.com" or host in _SHORT_HOSTS


def parse_spotify_link(value: str) -> Optional[tuple[str, str]]:
    """Return (kind, id) for open.spotify.com URLs and spotify: URIs, else None."""
    raw = (value or "").strip()
    m = _URI_RE.match(raw)
    if m:
        return m.group(1), m.group(2)
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    try:
        parsed = urlparse(raw)
    except ValueError:
        return None
    if (parsed.hostname or "").lower() != "open.spotify.com":
        return None
    parts = [p for p in parsed.path.split("/") if p]
    if parts and parts[0].startswith("intl-"):
        parts = parts[1:]
    if parts and parts[0] == "embed":
        parts = parts[1:]
    if len(parts) >= 2 and _ID_RE.match(parts[1]):
        return parts[0], parts[1]
    return None


async def _expand_short_link(session: aiohttp.ClientSession, url: str) -> tuple[str, str]:
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    async with session.get(url, allow_redirects=True) as resp:
        final_url = str(resp.url)
        parsed = parse_spotify_link(final_url)
        if parsed:
            return parsed
        # Some short links land on an HTML page that redirects via JS/meta.
        body = await resp.text(errors="replace")
    m = _OPEN_URL_IN_TEXT_RE.search(body)
    if m:
        return m.group(1), m.group(2)
    raise SpotifyError("could not expand the Spotify short link")


def _split_artists(subtitle: str) -> list[str]:
    return [a.strip() for a in subtitle.replace("\xa0", " ").split(",") if a.strip()]


def _parse_entity(kind: str, html: str) -> SpotifyItem:
    m = _NEXT_DATA_RE.search(html)
    if not m:
        raise SpotifyError("Spotify page has no track data (page layout changed?)")
    try:
        entity = json.loads(m.group(1))["props"]["pageProps"]["state"]["data"]["entity"]
    except (json.JSONDecodeError, KeyError, TypeError):
        raise SpotifyError("nothing found at this link (wrong ID, or Spotify changed its page)")

    name = str(entity.get("name") or entity.get("title") or "").strip()
    if kind == "track":
        artists = [str(a.get("name")).strip() for a in entity.get("artists") or [] if isinstance(a, dict) and a.get("name")]
        if not name:
            raise SpotifyError("Spotify track has no title")
        return SpotifyItem(kind, name, [SpotifyTrack(name, artists)])

    tracks: list[SpotifyTrack] = []
    for item in entity.get("trackList") or []:
        if not isinstance(item, dict):
            continue
        if item.get("entityType", "track") != "track":
            continue  # podcast episodes inside playlists
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        tracks.append(SpotifyTrack(title, _split_artists(str(item.get("subtitle") or ""))))
    if not tracks:
        raise SpotifyError(f"no tracks found in this Spotify {kind}")
    if kind == "artist":
        name = f"{name or 'artist'}: top tracks"
    return SpotifyItem(kind, name or kind, tracks)


async def resolve_spotify(value: str) -> SpotifyItem:
    """Resolve a Spotify link to its title and track list. Raises SpotifyError."""
    headers = {"User-Agent": _USER_AGENT, "Accept-Language": "en"}
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT, headers=headers) as session:
            parsed = parse_spotify_link(value)
            if parsed is None:
                parsed = await _expand_short_link(session, value.strip())
            kind, item_id = parsed
            if kind in UNSUPPORTED_KINDS:
                raise SpotifyError("podcasts and audiobooks are not supported")
            if kind not in SUPPORTED_KINDS:
                raise SpotifyError(f"unsupported Spotify link type: {kind}")

            url = f"https://open.spotify.com/embed/{kind}/{item_id}"
            async with session.get(url) as resp:
                if resp.status == 404:
                    raise SpotifyError(f"Spotify {kind} not found (private or deleted?)")
                if resp.status != 200:
                    raise SpotifyError(f"Spotify returned HTTP {resp.status}")
                html = await resp.text(errors="replace")
    except SpotifyError:
        raise
    except (aiohttp.ClientError, TimeoutError) as exc:
        raise SpotifyError(f"could not reach Spotify ({type(exc).__name__})") from exc

    item = _parse_entity(kind, html)
    logger.info("Spotify %s %s resolved: %r, %d track(s)", kind, item_id, item.name, len(item.tracks))
    return item
