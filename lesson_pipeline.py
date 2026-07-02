"""Shared pipeline for turning a YouTube video into a timestamped transcript
and a draft topic outline: download audio -> chunk -> Whisper transcribe ->
LLM group into topics.

Used by bot.py's automated lesson-ingestion flow (Этап 1) and by one-off
local scripts (e.g. transcribe_roadmap.py). Pure functions with explicit
dependencies (no env reads, no db/bot imports) so it can run standalone.

All heavy work here is synchronous (subprocess + blocking HTTP calls) —
callers on an asyncio event loop should run it via asyncio.to_thread.
"""

import json
import logging
import math
import random
import subprocess
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import groq
import yt_dlp
from groq import Groq

logger = logging.getLogger(__name__)

CHUNK_MB = 22  # stay safely under Groq's 25 MB Whisper upload limit
WHISPER_MODEL = "whisper-large-v3"
GROUPING_MODEL = "llama-3.3-70b-versatile"

# llama-3.3-70b-versatile's free-tier cap (12000 TPM) can't take a multi-hour
# transcript in one request, so group_into_topics() splits it into windows of
# real video time, with a small overlap so a topic isn't cut exactly at the
# boundary between two windows.
GROUP_WINDOW_SECONDS = 15 * 60
GROUP_WINDOW_OVERLAP_SECONDS = 90
GROUP_WINDOW_DELAY_SECONDS = 2.0  # pacing between sequential chunk calls

LLM_MAX_RETRIES = 5
LLM_RETRY_BASE_DELAY = 2.0  # seconds; doubles each attempt, plus jitter

# A Retry-After above this is not a short burst limit worth silently
# sleeping through — it's almost always Groq's daily token cap (TPD), which
# won't clear for the rest of the day regardless of how many times we retry.
# Fail fast with a clear message instead of blocking an admin's request for
# up to ~1.5h with zero feedback.
LLM_MAX_RETRY_DELAY = 90.0


class PipelineError(Exception):
    """Raised for any expected failure (bad link, ffmpeg/API error) so
    callers can show the admin a clean message instead of a raw traceback."""


def get_groq_client(groq_api_key: str) -> Groq:
    return Groq(api_key=groq_api_key)


def _format_wait(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}ч{m}м"
    if m:
        return f"{m}м{s}с"
    return f"{s}с"


def _call_groq_with_retry(fn, *, max_retries: int = LLM_MAX_RETRIES):
    """Call a Groq SDK function, retrying on 429 (rate limit) / 413 (payload
    too large) with exponential backoff — honoring a Retry-After header when
    Groq sends one. Any other error, or exhausting all retries, raises
    PipelineError so callers never see a raw SDK exception.

    Groq marks quota errors a short retry can't fix (most commonly the daily
    token cap, TPD — as opposed to the per-minute TPM burst limit) with
    `x-should-retry: false` and a Retry-After of tens of minutes to hours.
    Silently sleeping through that would block an admin's request for up to
    ~1.5h with zero feedback and indistinguishable from a hang, so those
    fail immediately with a clear message instead of being retried.
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except groq.APIStatusError as exc:
            if exc.status_code not in (429, 413):
                raise PipelineError(f"Groq API вернул ошибку {exc.status_code}: {exc}") from exc

            headers = exc.response.headers if exc.response is not None else {}
            should_retry = headers.get("x-should-retry")
            retry_after_raw = headers.get("retry-after")
            try:
                retry_after = float(retry_after_raw) if retry_after_raw else None
            except ValueError:
                retry_after = None

            if should_retry == "false" or (retry_after is not None and retry_after > LLM_MAX_RETRY_DELAY):
                wait_msg = f", повторить можно через {_format_wait(retry_after)}" if retry_after else ""
                raise PipelineError(
                    f"Groq API: лимит токенов исчерпан{wait_msg} ({exc})"
                ) from exc

            last_exc = exc
            if attempt == max_retries:
                break
            delay = retry_after if retry_after is not None else LLM_RETRY_BASE_DELAY * (2 ** attempt)
            delay += random.uniform(0, 1)  # jitter, avoid retry bursts lining back up
            logger.warning(
                "Groq API вернул %s, повтор через %.1fс (попытка %d/%d)",
                exc.status_code, delay, attempt + 1, max_retries,
            )
            time.sleep(delay)
    raise PipelineError(
        f"Groq API: превышен лимит запросов после {max_retries} попыток "
        f"(HTTP {getattr(last_exc, 'status_code', '?')})"
    ) from last_exc


def probe_video(youtube_url: str) -> dict:
    """Validate a YouTube link and return {"video_id", "title"} without
    downloading anything (equivalent to `yt-dlp --dump-json`)."""
    ydl_opts = {"quiet": True, "no_warnings": True, "skip_download": True}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(youtube_url, download=False)
    except yt_dlp.utils.DownloadError as exc:
        raise PipelineError(f"не удалось получить видео по ссылке ({exc})") from exc
    return {"video_id": info["id"], "title": info.get("title") or info["id"]}


def _download_raw_audio(youtube_url: str, out_dir: Path) -> Path:
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "format": "bestaudio/best",
        "outtmpl": str(out_dir / "source.%(ext)s"),
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(youtube_url, download=True)
        return Path(ydl.prepare_filename(info))


def extract_audio(source: Path, audio_out: Path) -> None:
    """Re-encode to mono 16kHz 64kbps mp3 — small enough to chunk cheaply
    while staying well within Whisper's accuracy range for speech."""
    cmd = [
        "ffmpeg", "-y", "-i", str(source),
        "-vn", "-ac", "1", "-ar", "16000", "-b:a", "64k",
        str(audio_out),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if r.returncode != 0:
        raise PipelineError(f"ffmpeg: не удалось извлечь аудио ({r.stderr[-500:]})")


def get_duration(path: Path) -> float:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True,
    )
    return float(r.stdout.strip())


def split_audio(audio: Path, chunk_dir: Path, chunk_bytes: int) -> list[tuple[Path, float]]:
    """Split audio into fixed-size chunks; return list of (chunk_path, start_sec)."""
    total_bytes = audio.stat().st_size
    duration_sec = get_duration(audio)
    bytes_per_sec = total_bytes / duration_sec
    chunk_sec = chunk_bytes / bytes_per_sec
    n_chunks = math.ceil(duration_sec / chunk_sec)

    chunks = []
    for i in range(n_chunks):
        start = i * chunk_sec
        out = chunk_dir / f"chunk_{i:03d}.mp3"
        cmd = [
            "ffmpeg", "-y", "-i", str(audio),
            "-ss", str(start), "-t", str(chunk_sec),
            "-c", "copy", str(out),
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        if r.returncode != 0:
            raise PipelineError(f"ffmpeg: не удалось разбить аудио на части ({r.stderr[-500:]})")
        chunks.append((out, start))
    return chunks


def transcribe_chunk(client: Groq, chunk_path: Path, offset_sec: float) -> list[dict]:
    """Return list of {start, end, text} with absolute (offset-adjusted) timestamps."""
    def _call():
        with open(chunk_path, "rb") as f:
            return client.audio.transcriptions.create(
                file=(chunk_path.name, f, "audio/mpeg"),
                model=WHISPER_MODEL,
                response_format="verbose_json",
                language="ru",
                timestamp_granularities=["segment"],
            )

    response = _call_groq_with_retry(_call)
    segments = []
    for seg in response.segments:
        # SDK may return dicts or objects depending on version
        if isinstance(seg, dict):
            s_start, s_end, s_text = seg["start"], seg["end"], seg["text"]
        else:
            s_start, s_end, s_text = seg.start, seg.end, seg.text
        segments.append({
            "start": round(offset_sec + s_start, 2),
            "end": round(offset_sec + s_end, 2),
            "text": s_text.strip(),
        })
    return segments


def download_and_transcribe(youtube_url: str, groq_api_key: str) -> list[dict]:
    """Full pipeline: download -> extract -> chunk -> transcribe -> merge.

    Returns a flat list of {start, end, text} segments in chronological order.
    """
    client = get_groq_client(groq_api_key)
    with TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        try:
            raw_audio = _download_raw_audio(youtube_url, tmp_dir)
        except yt_dlp.utils.DownloadError as exc:
            raise PipelineError(f"не удалось скачать аудио ({exc})") from exc

        audio_path = tmp_dir / "audio.mp3"
        extract_audio(raw_audio, audio_path)

        chunk_dir = tmp_dir / "chunks"
        chunk_dir.mkdir()
        chunks = split_audio(audio_path, chunk_dir, CHUNK_MB * 1024 * 1024)

        all_segments: list[dict] = []
        for chunk_path, offset in chunks:
            try:
                segs = transcribe_chunk(client, chunk_path, offset)
            except PipelineError:
                raise
            except Exception as exc:
                raise PipelineError(f"Whisper: ошибка транскрипции ({exc})") from exc
            all_segments.extend(segs)

    if not all_segments:
        raise PipelineError("транскрипция вернула пустой результат")
    return all_segments


def _parse_topics_response(data) -> list[dict]:
    """Extract/validate {"topics": [...]} JSON into a clean list of
    {"title": str, "start_seconds": int}. Silently drops malformed entries —
    callers decide whether an empty result is fatal."""
    topics = data.get("topics") if isinstance(data, dict) else None
    if not isinstance(topics, list):
        return []
    cleaned = []
    for t in topics:
        if not isinstance(t, dict):
            continue
        title = str(t.get("title", "")).strip()
        start = t.get("start_seconds")
        if not title or not isinstance(start, (int, float)):
            continue
        cleaned.append({"title": title[:200], "start_seconds": int(start)})
    return cleaned


def _dedupe_ascending(topics: list[dict]) -> list[dict]:
    """Sort by start_seconds and drop any entry that doesn't strictly
    advance past the previous one (duplicate/non-advancing timecodes)."""
    result: list[dict] = []
    for t in sorted(topics, key=lambda t: t["start_seconds"]):
        if result and t["start_seconds"] <= result[-1]["start_seconds"]:
            continue
        result.append(t)
    return result


def _chunk_segments_by_time(
    segments: list[dict], window_sec: float, overlap_sec: float
) -> list[list[dict]]:
    """Split segments into consecutive real-video-time windows, each
    extended by `overlap_sec` on both ends so a topic isn't cut exactly at
    a window boundary (the LLM sees a bit of the neighboring context)."""
    if not segments:
        return []
    total_end = segments[-1]["end"]
    windows: list[list[dict]] = []
    start = 0.0
    while start < total_end:
        window_end = start + window_sec
        lo = max(0.0, start - overlap_sec)
        hi = window_end + overlap_sec
        window_segs = [s for s in segments if lo <= s["start"] < hi]
        if window_segs:
            windows.append(window_segs)
        start = window_end
    return windows


def _group_window(client: Groq, video_title: str, segments: list[dict]) -> list[dict]:
    """One LLM call grouping a single (real-time-bounded) slice of the
    transcript into topics. Used both directly (short videos, one window)
    and repeatedly by group_into_topics for long ones."""
    transcript = "\n".join(f"[{int(seg['start'])}s] {seg['text']}" for seg in segments)
    window_start, window_end = int(segments[0]["start"]), int(segments[-1]["end"])

    system = (
        "Ты помогаешь разбить фрагмент транскрипта вебинара на осмысленные "
        "тематические блоки с таймкодами начала — как оглавление видео. "
        "Отвечай только валидным JSON."
    )
    user = (
        f"Видео: «{video_title}»\n\n"
        f"Ниже — фрагмент транскрипта, соответствующий интервалу "
        f"{window_start}-{window_end} секунд видео (это не всё видео целиком, "
        f"а один из последовательных кусков, на которые оно было разбито):\n\n"
        f"{transcript}\n\n"
        "Разбей этот фрагмент на последовательные смысловые темы (обычно 1-8 тем "
        "для фрагмента такой длины). Для каждой темы дай короткий заголовок на "
        "русском (до 80 символов) и start_seconds — целое число секунд, "
        "совпадающее с таймкодом ПЕРВОГО сегмента этой темы из списка выше. "
        "Темы должны идти строго по возрастанию start_seconds.\n\n"
        'Ответь строго в формате JSON: {"topics": [{"title": "...", "start_seconds": 0}]}'
    )

    def _call():
        return client.chat.completions.create(
            model=GROUPING_MODEL,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            response_format={"type": "json_object"},
            temperature=0.2,
            max_tokens=1500,
        )

    try:
        resp = _call_groq_with_retry(_call)
        data = json.loads(resp.choices[0].message.content)
    except PipelineError:
        raise
    except Exception as exc:
        raise PipelineError(f"LLM: не удалось сгруппировать темы ({exc})") from exc

    return _dedupe_ascending(_parse_topics_response(data))


def _collapse_duplicate_topics(client: Groq, video_title: str, topics: list[dict]) -> list[dict]:
    """Final pass over the merged topic list from all windows: ask the LLM
    to collapse any topic that got split into two near-duplicate entries at
    a window boundary. Falls back to the un-collapsed (already deduped)
    list if this call fails for any reason — it's a polish step, not worth
    failing the whole pipeline over."""
    listing = "\n".join(f"{i}. [{t['start_seconds']}s] {t['title']}" for i, t in enumerate(topics))
    system = (
        "Ты редактируешь черновой список тем видео, собранный по кускам транскрипта — "
        "соседние темы на границах кусков иногда дублируют друг друга или описывают "
        "один и тот же смысловой блок. Отвечай только валидным JSON."
    )
    user = (
        f"Видео: «{video_title}»\n\n"
        f"Черновой список тем (номер, таймкод, заголовок):\n\n{listing}\n\n"
        "Если две соседние темы по сути об одном и том же (одна тема была случайно "
        "разбита на две при склейке кусков транскрипта) — объедини их в одну, оставив "
        "более ранний start_seconds и наиболее точный заголовок. Не объединяй темы, "
        "которые действительно про разное. Верни итоговый список по возрастанию "
        "start_seconds.\n\n"
        'Ответь строго в формате JSON: {"topics": [{"title": "...", "start_seconds": 0}]}'
    )

    def _call():
        return client.chat.completions.create(
            model=GROUPING_MODEL,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=4000,
        )

    try:
        resp = _call_groq_with_retry(_call)
        data = json.loads(resp.choices[0].message.content)
        collapsed = _dedupe_ascending(_parse_topics_response(data))
    except Exception as exc:
        logger.warning("Collapse pass failed (%s) — keeping un-collapsed topic list", exc)
        return topics

    return collapsed if collapsed else topics


def group_into_topics(video_title: str, segments: list[dict], groq_api_key: str) -> list[dict]:
    """LLM pass: turn a flat transcript into a topic outline with timecodes.

    Long transcripts are split into ~15-minute (real video time) overlapping
    windows and grouped one window at a time — llama-3.3-70b-versatile's
    12000 TPM cap can't take a multi-hour transcript in a single request.
    Results are merged by timecode and, if there was more than one window,
    passed through one more LLM call to collapse any topic that got split
    across a window boundary.

    Returns a list of {"title": str, "start_seconds": int}, strictly
    ascending by start_seconds, each start_seconds grounded in an actual
    segment start (the model is instructed to copy one, not invent one).
    """
    client = get_groq_client(groq_api_key)
    windows = _chunk_segments_by_time(segments, GROUP_WINDOW_SECONDS, GROUP_WINDOW_OVERLAP_SECONDS)
    if not windows:
        raise PipelineError("транскрипт пуст — нечего группировать")

    all_topics: list[dict] = []
    for i, window_segments in enumerate(windows):
        all_topics.extend(_group_window(client, video_title, window_segments))
        if i < len(windows) - 1:
            time.sleep(GROUP_WINDOW_DELAY_SECONDS)

    merged = _dedupe_ascending(all_topics)
    if len(windows) > 1 and len(merged) > 1:
        merged = _dedupe_ascending(_collapse_duplicate_topics(client, video_title, merged))

    if not merged:
        raise PipelineError("LLM не вернул ни одной темы")
    return merged
