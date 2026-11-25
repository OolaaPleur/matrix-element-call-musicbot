import asyncio
import hashlib
import json
import logging
import os
import shutil
import subprocess
import wave
from collections import deque
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


logger = logging.getLogger(__name__)

YTDLP_AUDIO_FORMAT_MAP = {
    "wav": "wav",
    "mp3": "mp3",
    "ogg": "vorbis",
    "m4a": "m4a",
    "opus": "opus",
}

YTDLP_JS_RUNTIME = ["--js-runtimes", "node"]
# One extraction gives both the metadata and a direct stream URL (top-level "url"
# of the selected format). "/best": sites without an audio-only format.
YTDLP_STREAM_FORMAT = "bestaudio/best"
INFO_CACHE_MAX = 256
# Query parameters of a YouTube watch URL that change what is played.
YOUTUBE_KEPT_PARAMS = {"v", "list", "index"}


class AudioQueue:
    """Manages audio download queue with caching and pre-roll silence."""

    def __init__(
        self,
        audio_dir: Path,
        auto_advance_buffer: float = 2.0,
        preroll_silence: float = 8.0,
        cache_mode: str = "size_lru",
        cache_max_bytes: int = 1_073_741_824,
        cache_delete_after_playback: bool = True,
        cache_delete_on_shutdown: bool = True,
        search_mode: str = "fast",
        search_timeout_seconds: float = 8.0,
        extractor_retries: int = 1,
        download_format: str = "wav",
        audio_quality: str = "best",
        cookies_file: Optional[str] = None,
        proxy: str = "",
    ):
        self.audio_dir = audio_dir
        self.audio_dir.mkdir(parents=True, exist_ok=True)
        self.queue = deque()
        self.current = None
        self.loop_mode = False
        self.auto_advance_buffer = auto_advance_buffer
        self.preroll_silence = preroll_silence
        self.cache_mode = str(cache_mode or "size_lru").strip().lower()
        if self.cache_mode not in {"size_lru", "never_delete", "always_delete"}:
            self.cache_mode = "size_lru"
        self.cache_max_bytes = max(0, int(cache_max_bytes))
        self.cache_delete_after_playback = bool(cache_delete_after_playback)
        self.cache_delete_on_shutdown = bool(cache_delete_on_shutdown)
        self.search_mode = str(search_mode or "fast").strip().lower()
        if self.search_mode not in {"fast", "accurate"}:
            self.search_mode = "fast"
        self.search_timeout_seconds = max(0.0, float(search_timeout_seconds))
        self.extractor_retries = max(0, int(extractor_retries))
        self.download_format = str(download_format or "wav").strip().lower()
        if self.download_format not in {"wav", "mp3", "ogg", "m4a", "opus"}:
            self.download_format = "wav"
        self.ytdlp_audio_format = YTDLP_AUDIO_FORMAT_MAP[self.download_format]
        self.audio_quality = str(audio_quality or "best").strip().lower()
        if self.audio_quality not in {"best", "medium", "worst"}:
            self.audio_quality = "best"
        self.cookies_file = cookies_file or None
        self.proxy = proxy

        # Cache shape: {url: {"file": str, "music_duration": Optional[float]}}
        self.download_cache = {}
        # One download at a time per query/URL: concurrent downloads of the same
        # track write the same temp file and both fail (seen with stream prefetch
        # + stream-failure fallback, 2026-11-15).
        self._download_locks: dict[str, asyncio.Lock] = {}
        self.search_cache: dict[str, dict] = {}
        # source_url -> resolved metadata (without the expiring stream URL), so the
        # download after a stream start doesn't run yt-dlp's extraction again.
        self._info_cache: dict[str, dict] = {}
        self._enforce_size_limit()

    def _cookies_args(self) -> list[str]:
        return ["--cookies", self.cookies_file] if self.cookies_file else []

    def _yt_dlp_proxy_cla(self) -> list[str]:
        if self.proxy is None or self.proxy == "":
            return []
        return ["--proxy", self.proxy]

    def _is_cache_audio_path(self, path: Path) -> bool:
        if not path.is_file():
            return False
        if path.name.endswith(".part"):
            return False
        if "_temp" in path.stem:
            return False
        return path.suffix.lower() in {".wav", ".mp3", ".ogg", ".m4a", ".opus"}

    def _iter_cache_audio_files(self) -> list[Path]:
        files: list[Path] = []
        for path in self.audio_dir.iterdir():
            try:
                if self._is_cache_audio_path(path):
                    files.append(path)
            except OSError:
                continue
        return files

    def _in_use_files(self) -> set[str]:
        in_use: set[str] = set()
        if isinstance(self.current, dict):
            file_path = self.current.get("file")
            if isinstance(file_path, str) and file_path:
                in_use.add(file_path)
        for track in self.queue:
            if not isinstance(track, dict):
                continue
            file_path = track.get("file")
            if isinstance(file_path, str) and file_path:
                in_use.add(file_path)
        return in_use

    def _is_within_audio_dir(self, file_path: str) -> bool:
        try:
            resolved_file = Path(file_path).resolve()
            resolved_dir = self.audio_dir.resolve()
        except Exception:
            return False
        return resolved_dir == resolved_file or resolved_dir in resolved_file.parents

    def _drop_cache_entries_for_file(self, file_path: str):
        stale = [url for url, info in self.download_cache.items() if info.get("file") == file_path]
        for url in stale:
            self.download_cache.pop(url, None)

    def _delete_file(self, file_path: str) -> bool:
        if not file_path or not self._is_within_audio_dir(file_path):
            return False
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info("Deleted cached audio file: %s", file_path)
            self._drop_cache_entries_for_file(file_path)
            return True
        except Exception as exc:
            logger.warning("Failed deleting cached audio file %s: %s", file_path, exc)
            return False

    def _touch_file(self, file_path: str):
        try:
            os.utime(file_path, None)
        except Exception:
            pass

    def _enforce_size_limit(self):
        if self.cache_mode != "size_lru":
            return
        if self.cache_max_bytes <= 0:
            return

        files: list[tuple[Path, os.stat_result]] = []
        total_bytes = 0
        for path in self._iter_cache_audio_files():
            try:
                stat = path.stat()
            except OSError:
                continue
            if not path.is_file():
                continue
            files.append((path, stat))
            total_bytes += int(stat.st_size)

        if total_bytes <= self.cache_max_bytes:
            return

        in_use = self._in_use_files()
        candidates = sorted(files, key=lambda item: item[1].st_mtime)
        for path, stat in candidates:
            if total_bytes <= self.cache_max_bytes:
                break
            file_path = str(path)
            if file_path in in_use:
                continue
            if self._delete_file(file_path):
                total_bytes -= int(stat.st_size)

    def maybe_delete_track_file(self, track: Optional[dict], *, trigger: str):
        if not isinstance(track, dict):
            return

        should_delete = bool(track.get("non_cacheable"))
        if not should_delete:
            if self.cache_mode != "always_delete":
                return
            if trigger == "after_playback" and not self.cache_delete_after_playback:
                return
            if trigger == "shutdown" and not self.cache_delete_on_shutdown:
                return
            should_delete = True

        if not should_delete:
            return

        file_path = track.get("file")
        if not isinstance(file_path, str) or not file_path:
            return

        if file_path in self._in_use_files():
            return
        self._delete_file(file_path)

    def cleanup_on_shutdown(self):
        if self.cache_mode != "always_delete" or not self.cache_delete_on_shutdown:
            return

        for path in self._iter_cache_audio_files():
            self._delete_file(str(path))

    @staticmethod
    def normalize_title(title: str) -> str:
        return " ".join(title.split())

    def get_audio_quality_for_ytdlp(self) -> str:
        quality_map = {
            "best": "0",
            "medium": "5",
            "worst": "10",
        }
        return quality_map.get(self.audio_quality, "0")

    @staticmethod
    def get_audio_duration(wav_file: str) -> Optional[float]:
        """Get duration of audio file in seconds."""
        try:
            if str(wav_file).lower().endswith(".wav"):
                with wave.open(wav_file, "rb") as wav_file_handle:
                    frames = wav_file_handle.getnframes()
                    rate = wav_file_handle.getframerate()
                    return frames / float(rate)

            output = subprocess.check_output(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration",
                    "-of",
                    "default=noprint_wrappers=1:nokey=1",
                    str(wav_file),
                ],
                stderr=subprocess.STDOUT,
                text=True,
                timeout=5,
            ).strip()
            if not output:
                return None
            value = float(output)
            if value <= 0:
                return None
            return value
        except Exception as exc:
            logger.error(f"Error getting duration: {exc}")
            return None

    @staticmethod
    def add_silence_to_wav(input_file: str, output_file: str, silence_duration: float) -> bool:
        """Prepend silence to an audio file."""
        try:
            if not str(output_file).lower().endswith(".wav"):
                delay_ms = max(0, int(round(float(silence_duration) * 1000.0)))
                cmd = [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-y",
                    "-i",
                    input_file,
                    "-af",
                    f"adelay={delay_ms}:all=1",
                    output_file,
                ]
                subprocess.check_output(
                    cmd,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=30,
                )
                logger.info(f"Added {silence_duration:.1f}s pre-roll silence to {output_file}")
                return True

            with wave.open(input_file, "rb") as input_wav:
                params = input_wav.getparams()
                original_frames = input_wav.readframes(params.nframes)

            silence_frames_count = int(params.framerate * silence_duration)
            silence_data = b"\x00" * (silence_frames_count * params.nchannels * params.sampwidth)

            with wave.open(output_file, "wb") as output_wav:
                output_wav.setparams(params)
                output_wav.writeframes(silence_data + original_frames)

            logger.info(f"Added {silence_duration:.1f}s pre-roll silence to {output_file}")
            return True
        except Exception as exc:
            logger.error(f"Error adding silence: {exc}")
            return False

    @staticmethod
    def normalize_media_url(value: str) -> str:
        raw = (value or "").strip()
        if not raw:
            return raw

        # Prepend https:// when the user omits the scheme (e.g. "youtube.com/watch?v=...")
        if not raw.startswith(("http://", "https://")) and "/" in raw and not raw.startswith("ytsearch"):
            candidate = "https://" + raw
            try:
                p = urlparse(candidate)
                if p.scheme in {"http", "https"} and p.netloc and "." in p.netloc:
                    raw = candidate
            except Exception:
                pass

        try:
            parsed = urlparse(raw)
        except Exception:
            return raw

        host = (parsed.hostname or "").lower()
        youtube_hosts = {"youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com"}
        path_parts = [part for part in parsed.path.split("/") if part]
        # One spelling per video, as yt-dlp's webpage_url has it: the cache is keyed
        # by it. Share links carry ?si=/&t=/&feature=, which would miss the cache.
        if host == "youtu.be" and parsed.path.strip("/"):
            video_id = parsed.path.strip("/").split("/")[0]
            pairs = [("v", video_id), *parse_qsl(parsed.query, keep_blank_values=True)]
        elif host in youtube_hosts and len(path_parts) == 2 and path_parts[0] in {"shorts", "live", "embed"}:
            pairs = [("v", path_parts[1]), *parse_qsl(parsed.query, keep_blank_values=True)]
        elif host in youtube_hosts and parsed.path == "/watch":
            pairs = parse_qsl(parsed.query, keep_blank_values=True)
        elif host == "music.youtube.com":
            return urlunparse(parsed._replace(netloc="www.youtube.com"))
        else:
            return raw
        kept = [(k, v) for k, v in pairs if k in YOUTUBE_KEPT_PARAMS]
        return urlunparse(("https", "www.youtube.com", "/watch", "", urlencode(kept, doseq=True), ""))

    @staticmethod
    def is_single_video_url(url: str) -> bool:
        """A canonical YouTube video URL (see normalize_media_url): yt-dlp's
        webpage_url is the same string, so the file's cache name is known upfront."""
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        if parsed.netloc != "www.youtube.com" or parsed.path != "/watch":
            return False
        keys = {k for k, _ in parse_qsl(parsed.query)}
        return keys == {"v"}

    @staticmethod
    def looks_like_url(value: str) -> bool:
        try:
            parsed = urlparse(value)
        except Exception:
            return False
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)

    @staticmethod
    async def _run_command(*cmd: str) -> tuple[int, str, str]:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        return (process.returncode if process.returncode is not None else -1), stdout.decode("utf-8", errors="replace"), stderr.decode("utf-8", errors="replace")

    async def _resolve_media_info(self, dlp_cmd: str, query_or_url: str) -> tuple[bool, dict | str]:
        is_url = self.looks_like_url(query_or_url)
        target = query_or_url if is_url else f"ytsearch1:{query_or_url}"
        cmd = [
            dlp_cmd,
            "--no-playlist",
            # A channel or tab URL is a playlist that --no-playlist doesn't reduce:
            # without -I 1 yt-dlp extracts every video in it (minutes, hundreds of
            # requests) only for the first one to be used.
            "--playlist-items",
            "1",
            "--dump-single-json",
            "-f",
            YTDLP_STREAM_FORMAT,
            "--extractor-retries",
            str(self.extractor_retries),
            *YTDLP_JS_RUNTIME,
        ]
        cmd.extend(self._cookies_args())
        if self.search_mode == "fast":
            cmd.extend(["--no-warnings", "--socket-timeout", str(max(3.0, self.search_timeout_seconds))])
        cmd.extend(self._yt_dlp_proxy_cla())
        cmd.append(target)

        code, stdout, stderr = await self._run_command(*cmd)
        if code != 0:
            return False, (stderr.strip() or "Failed to resolve media info")

        try:
            data = json.loads(stdout)
        except json.JSONDecodeError:
            return False, "Could not parse media metadata"
        ok, info = self._info_from_json(data, query_or_url)
        if ok and isinstance(info, dict) and self.looks_like_url(query_or_url):
            self._remember_info(info, alias=query_or_url)
        return ok, info

    def _info_from_json(self, data: dict, query_or_url: str) -> tuple[bool, dict | str]:
        is_url = self.looks_like_url(query_or_url)
        if not isinstance(data, dict):
            return False, "Could not parse media metadata"
        entry = data
        entries = data.get("entries")
        if isinstance(entries, list) and entries:
            for item in entries:
                if isinstance(item, dict) and item.get("webpage_url"):
                    entry = item
                    break
            else:
                if isinstance(entries[0], dict):
                    entry = entries[0]

        source_url = entry.get("webpage_url") or entry.get("original_url") or (query_or_url if is_url else None)
        if not source_url:
            return False, "No playable result found"
        source_url = self.normalize_media_url(str(source_url))

        raw_title = entry.get("title") or os.path.basename(source_url)
        title = self.normalize_title(str(raw_title))
        duration = entry.get("duration")
        uploader = entry.get("uploader") or entry.get("channel")
        stream_url = entry.get("url")
        if not isinstance(stream_url, str) or not stream_url.strip():
            stream_url = None

        out = {
            "source_url": source_url,
            "title": title,
            "duration": float(duration) if isinstance(duration, (int, float)) else None,
            "uploader": uploader,
            "stream_url": stream_url,
            "artist": entry.get("artist") or "",
            "track": entry.get("track") or "",
            "album": entry.get("album") or "",
            "channel": entry.get("channel") or entry.get("uploader") or "",
        }
        self._remember_info(out)
        return True, out

    def _remember_info(self, info: dict, alias: Optional[str] = None):
        cached = {k: v for k, v in info.items() if k != "stream_url"}
        for key in {info["source_url"], alias or info["source_url"]}:
            self._info_cache.pop(key, None)
            self._info_cache[key] = cached
        while len(self._info_cache) > INFO_CACHE_MAX:
            self._info_cache.pop(next(iter(self._info_cache)))

    async def resolve_stream_source(self, query_or_url: str) -> tuple[bool, dict | str]:
        query_or_url = query_or_url.strip()
        if not query_or_url:
            return False, "Empty query"
        query_or_url = self.normalize_media_url(query_or_url)

        dlp_cmd = "yt-dlp" if shutil.which("yt-dlp") else "youtube-dlp"
        if not shutil.which(dlp_cmd):
            return False, "yt-dlp (or youtube-dlp) is not installed"

        resolved_ok, resolved = await self._resolve_media_info(dlp_cmd, query_or_url)
        if not resolved_ok:
            return False, resolved
        if not isinstance(resolved, dict) or not resolved.get("stream_url"):
            return False, "No stream source available"

        return True, {
            "stream_url": resolved["stream_url"],
            "source_url": resolved.get("source_url") or query_or_url,
            "title": resolved.get("title") or os.path.basename(query_or_url) or "track",
            "duration": resolved.get("duration"),
            "uploader": resolved.get("uploader"),
            "artist":  resolved.get("artist",  ""),
            "track":   resolved.get("track",   ""),
            "album":   resolved.get("album",   ""),
            "channel": resolved.get("channel", ""),
        }

    async def resolve_playlist_entries(self, url: str) -> tuple[bool, dict | str]:
        """Resolve playlist metadata and entry URLs for a playlist URL.

        Returns:
            (success, result)

            Success result shape:
            {
                "is_playlist": bool,
                "title": Optional[str],
                "entries": list[str],
            }
        """
        candidate = self.normalize_media_url((url or "").strip())
        if not self.looks_like_url(candidate):
            return True, {"is_playlist": False, "title": None, "entries": []}

        dlp_cmd = "yt-dlp" if shutil.which("yt-dlp") else "youtube-dlp"
        if not shutil.which(dlp_cmd):
            return False, "yt-dlp (or youtube-dlp) is not installed"

        cmd = [
            dlp_cmd,
            "--flat-playlist",
            "--dump-single-json",
            "--extractor-retries",
            str(self.extractor_retries),
            *YTDLP_JS_RUNTIME,
        ]
        cmd.extend(self._cookies_args())
        if self.search_mode == "fast":
            cmd.extend(["--no-warnings", "--lazy-playlist", "--socket-timeout", str(max(3.0, self.search_timeout_seconds))])
        cmd.extend(self._yt_dlp_proxy_cla())
        cmd.append(candidate)

        code, stdout, stderr = await self._run_command(*cmd)
        if code != 0:
            return False, (stderr.strip() or "Failed to resolve playlist metadata")

        try:
            data = json.loads(stdout)
        except json.JSONDecodeError:
            return False, "Could not parse playlist metadata"

        if not isinstance(data, dict):
            return True, {"is_playlist": False, "title": None, "entries": []}

        entries = data.get("entries")
        if not isinstance(entries, list) or not entries:
            return True, {"is_playlist": False, "title": None, "entries": []}
        # A channel URL lists its tabs (/videos, /streams, /shorts), each a nested
        # playlist: queue the videos of the first tab, not the tabs themselves.
        tabs = [e for e in entries if isinstance(e, dict) and e.get("_type") == "playlist"]
        if tabs:
            entries = next((tab["entries"] for tab in tabs if isinstance(tab.get("entries"), list) and tab["entries"]), [])
            if not entries:
                return True, {"is_playlist": False, "title": None, "entries": []}

        urls: list[str] = []
        seen: set[str] = set()
        for item in entries:
            if not isinstance(item, dict):
                continue

            webpage_url = item.get("webpage_url")
            raw_url = webpage_url if isinstance(webpage_url, str) and webpage_url else item.get("url")
            if not isinstance(raw_url, str) or not raw_url.strip():
                continue

            normalized = self.normalize_media_url(raw_url.strip())
            if not self.looks_like_url(normalized):
                entry_id = item.get("id")
                if isinstance(entry_id, str) and entry_id:
                    normalized = f"https://www.youtube.com/watch?v={entry_id}"
                else:
                    continue

            if normalized in seen:
                continue
            seen.add(normalized)
            urls.append(normalized)

        title_value = data.get("title")
        title = self.normalize_title(str(title_value)) if isinstance(title_value, str) else None
        return True, {"is_playlist": True, "title": title, "entries": urls}

    async def download_audio(self, query_or_url: str) -> tuple[bool, dict | str]:
        """Download audio, serialized per query/URL (a second caller gets the cached file)."""
        key = self.normalize_media_url(query_or_url.strip()).casefold()
        # Locks are never removed (a few bytes per distinct query): dropping one while
        # a waiter is about to take it would let a third caller run in parallel.
        lock = self._download_locks.setdefault(key, asyncio.Lock())
        async with lock:
            return await self._download_audio_unlocked(query_or_url)

    def _cache_path(self, source_url: str) -> Path:
        url_hash = hashlib.md5(source_url.encode(), usedforsecurity=False).hexdigest()[:8]
        return self.audio_dir / f"{url_hash}.{self.download_format}"

    def _has_local_copy(self, source_url: str) -> bool:
        cached = self.download_cache.get(source_url)
        if cached and os.path.exists(cached.get("file") or ""):
            return True
        # Files from before a restart are reused only without pre-roll (see _cached_result).
        return self.preroll_silence <= 0 and self._cache_path(source_url).is_file()

    def _result(self, file_path: str, duration: Optional[float], resolved: dict, non_cacheable: bool) -> dict:
        return {
            "file": file_path,
            "duration": duration,
            "title": resolved["title"],
            "source_url": resolved["source_url"],
            "uploader": resolved.get("uploader"),
            "non_cacheable": non_cacheable,
            "artist":  resolved.get("artist",  ""),
            "track":   resolved.get("track",   ""),
            "album":   resolved.get("album",   ""),
            "channel": resolved.get("channel", ""),
        }

    def _exceeds_cache(self, file_path: str) -> bool:
        if self.cache_mode != "size_lru" or self.cache_max_bytes <= 0:
            return False
        try:
            return os.path.getsize(file_path) > self.cache_max_bytes
        except OSError:
            return False

    async def _cached_result(self, resolved: dict) -> Optional[dict]:
        source_url = resolved["source_url"]
        cached = self.download_cache.get(source_url)
        if cached:
            cached_file = cached.get("file")
            if isinstance(cached_file, str) and os.path.exists(cached_file):
                self._touch_file(cached_file)
                logger.info(f"Using cached file: {cached_file}")
                return self._result(cached_file, cached["music_duration"], resolved, self._exceeds_cache(cached_file))
            self.download_cache.pop(source_url, None)

        # Downloaded before a restart: files are named after the URL, only the
        # in-memory index was lost.
        final_output_path = self._cache_path(source_url)
        if final_output_path.is_file() and self.preroll_silence <= 0:
            final_output = str(final_output_path)
            music_duration = await asyncio.to_thread(self.get_audio_duration, final_output)
            if music_duration is not None:
                self.download_cache[source_url] = {"file": final_output, "music_duration": music_duration}
                self._touch_file(final_output)
                logger.info("Using cached file from disk: %s", final_output)
                return self._result(final_output, music_duration, resolved, False)
        return None

    async def _download_audio_unlocked(self, query_or_url: str) -> tuple[bool, dict | str]:
        """Download audio from URL/search with caching and pre-roll silence.

        Returns (success, result dict from _result() or an error message).
        yt-dlp runs: a search query = metadata + download (the metadata tells
        whether the track is cached); a URL seen before or already on disk =
        metadata only if needed; a new URL = one run that downloads and prints
        the metadata.
        """
        query_or_url = query_or_url.strip()
        if not query_or_url:
            return False, "Empty query"
        query_or_url = self.normalize_media_url(query_or_url)

        dlp_cmd = "yt-dlp" if shutil.which("yt-dlp") else "youtube-dlp"
        if not shutil.which(dlp_cmd):
            return False, "yt-dlp (or youtube-dlp) is not installed"

        query_key = query_or_url.casefold()
        is_direct_url = self.looks_like_url(query_or_url)
        resolved: Optional[dict] = None
        if self.search_mode == "fast" and not is_direct_url and query_key in self.search_cache:
            resolved = dict(self.search_cache[query_key])
        elif is_direct_url and query_or_url in self._info_cache:
            resolved = dict(self._info_cache[query_or_url])
        # Metadata first unless it's a plain video URL: for other URLs (playlists,
        # other sites) the cache name isn't known until yt-dlp names the source,
        # so the disk check after a restart would miss and download again.
        elif (not is_direct_url or self._has_local_copy(query_or_url)
              or not self.is_single_video_url(query_or_url)):
            resolved_ok, info = await self._resolve_media_info(dlp_cmd, query_or_url)
            if not resolved_ok:
                return False, info
            if not isinstance(info, dict):
                return False, "Resolved metadata is invalid"
            resolved = info

        if resolved is not None:
            if self.search_mode == "fast" and not is_direct_url:
                self.search_cache[query_key] = dict(resolved)
            hit = await self._cached_result(resolved)
            if hit:
                return True, hit
            return await self._fetch_audio(dlp_cmd, resolved["source_url"], resolved)
        return await self._fetch_audio(dlp_cmd, query_or_url, None)

    async def _fetch_audio(self, dlp_cmd: str, url: str, resolved: Optional[dict]) -> tuple[bool, dict | str]:
        """Run the yt-dlp download; with resolved=None it also prints the metadata (-j)."""
        url_hash = hashlib.md5(url.encode(), usedforsecurity=False).hexdigest()[:8]
        temp_output = str(self.audio_dir / f"{url_hash}_temp.%(ext)s")

        cmd = [dlp_cmd]
        if self.search_mode == "fast":
            cmd += ["--no-warnings", "--socket-timeout", str(max(3.0, self.search_timeout_seconds))]
        cmd += self._cookies_args()
        cmd += self._yt_dlp_proxy_cla()
        if resolved is None:
            cmd += ["--dump-json", "--no-simulate"]
        cmd += [
            "--no-playlist",
            "--playlist-items",
            "1",  # never more than one file, whatever the URL turns out to be
            "-x",
            "--audio-format",
            self.ytdlp_audio_format,
            "--audio-quality",
            self.get_audio_quality_for_ytdlp(),
            "--extractor-retries",
            str(self.extractor_retries),
            "-o",
            temp_output,
            *YTDLP_JS_RUNTIME,
            url,
        ]

        logger.info(
            "Audio download selected: format=%s (yt-dlp=%s), quality=%s%s",
            self.download_format,
            self.ytdlp_audio_format,
            self.audio_quality,
            "" if resolved else ", metadata in the same run",
        )

        try:
            process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                error_msg = stderr.decode(errors="replace").strip() if stderr else "Unknown error"
                logger.error(f"yt-dlp failed: {error_msg}")
                return False, error_msg

            if resolved is None:
                info_line = next(
                    (line for line in reversed(stdout.decode("utf-8", errors="replace").splitlines())
                     if line.startswith("{")),
                    None,
                )
                try:
                    data = json.loads(info_line) if info_line else None
                except json.JSONDecodeError:
                    data = None
                ok, info = self._info_from_json(data, url) if data else (False, "Could not parse media metadata")
                if not ok or not isinstance(info, dict):
                    return False, info
                # A URL spelled differently from webpage_url: remember it too, or the
                # next request for it would download the file again.
                self._remember_info(info, alias=url)
                resolved = info
                if info["source_url"] != url:
                    # Turned out to be another spelling of a track we already have.
                    hit = await self._cached_result(info)
                    if hit:
                        for leftover in self.audio_dir.glob(f"{url_hash}_temp.*"):
                            leftover.unlink(missing_ok=True)
                        return True, hit

            temp_candidates = [
                p
                for p in self.audio_dir.glob(f"{url_hash}_temp.*")
                if p.is_file() and not p.name.endswith(".part")
            ]
            preferred_ext = f".{self.download_format}"
            temp_file = next((p for p in temp_candidates if p.suffix.lower() == preferred_ext), None)
            if temp_file is None and temp_candidates:
                temp_file = temp_candidates[0]

            if temp_file is None or not temp_file.exists():
                return False, "File not found after download"

            source_url = resolved["source_url"]
            final_output_path = self._cache_path(source_url)
            final_output = str(final_output_path)
            music_duration = await asyncio.to_thread(self.get_audio_duration, str(temp_file))

            if self.preroll_silence > 0:
                # Into a temp name first: the final path may be playing right now (a
                # track queued twice), and a half-written file must never look cached.
                preroll_output = final_output_path.with_name(
                    f"{final_output_path.stem}.preroll{final_output_path.suffix}"
                )
                silence_ok = await asyncio.to_thread(
                    self.add_silence_to_wav, str(temp_file), str(preroll_output), self.preroll_silence
                )
                if silence_ok:
                    os.replace(preroll_output, final_output)
                else:
                    preroll_output.unlink(missing_ok=True)
                    logger.warning("Failed to add pre-roll silence, using original file")
                    temp_file.rename(final_output)
            else:
                temp_file.rename(final_output)

            for leftover in temp_candidates:
                if leftover == final_output_path:
                    continue
                if leftover.exists():
                    try:
                        leftover.unlink()
                    except OSError:
                        pass

            non_cacheable = self._exceeds_cache(final_output)
            if non_cacheable:
                logger.info(
                    "Downloaded file exceeds cache_max_bytes; will delete after playback: %s",
                    final_output,
                )
            else:
                self.download_cache[source_url] = {"file": final_output, "music_duration": music_duration}

            self._touch_file(final_output)
            self._enforce_size_limit()

            if music_duration is not None:
                logger.info(
                    f"Downloaded: {final_output} "
                    f"(music: {music_duration:.2f}s + pre-roll: {self.preroll_silence:.2f}s)"
                )
            else:
                logger.info(
                    f"Downloaded: {final_output} "
                    f"(music duration unknown + pre-roll: {self.preroll_silence:.2f}s)"
                )

            return True, self._result(final_output, music_duration, resolved, non_cacheable)
        except Exception as exc:
            logger.error(f"Download exception: {exc}")
            return False, str(exc)

    def add_to_queue(
        self,
        audio_file: str,
        title: Optional[str] = None,
        duration: Optional[float] = None,
        source_url: Optional[str] = None,
        non_cacheable: bool = False,
        artist: str = "",
        track: str = "",
        album: str = "",
        channel: str = "",
        uploader: str = "",
    ):
        """Add a track to queue."""
        track_entry = {
            "file": audio_file,
            "title": title or os.path.basename(audio_file),
            "duration": duration,
            "source_url": source_url,
            "non_cacheable": bool(non_cacheable),
            "artist": artist,
            "track": track,
            "album": album,
            "channel": channel,
            "uploader": uploader,
        }
        self.queue.append(track_entry)
        if duration is not None:
            logger.info(f"Added to queue: {track_entry['title']} (duration: {duration:.2f}s)")
        else:
            logger.info(f"Added to queue: {track_entry['title']}")

    def get_next(self) -> Optional[dict]:
        """Pop next track from queue and set current."""
        if not self.queue:
            self.current = None
            return None
        self.current = self.queue.popleft()
        return self.current

    def get_current_or_next(self) -> Optional[dict]:
        """Return current track in loop mode, otherwise pop next."""
        if self.loop_mode and self.current:
            return self.current
        return self.get_next()

    def clear_queue(self):
        self.queue.clear()
        logger.info("Queue cleared")

    def has_source(self, source_url: str) -> bool:
        if not source_url:
            return False

        if self.current and self.current.get("source_url") == source_url:
            return True

        return any(item.get("source_url") == source_url for item in self.queue)

    def toggle_loop(self) -> bool:
        self.loop_mode = not self.loop_mode
        return self.loop_mode
