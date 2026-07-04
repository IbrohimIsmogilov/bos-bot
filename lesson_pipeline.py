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
import openai
import yt_dlp
from groq import Groq
from openai import OpenAI

logger = logging.getLogger(__name__)

CHUNK_MB = 22  # stay safely under Groq's 25 MB Whisper upload limit
WHISPER_MODEL = "whisper-large-v3"

# Topic grouping runs on Cerebras (OpenAI-compatible endpoint), not Groq —
# its free tier gives 1,000,000 tokens/day vs. Groq's 100,000, which a
# multi-hour transcript can burn through in a single run. Whisper
# transcription stays on Groq (see get_groq_client/transcribe_chunk).
CEREBRAS_BASE_URL = "https://api.cerebras.ai/v1"
GROUPING_MODEL = "gpt-oss-120b"

# The transcript is split into windows of real video time and grouped one
# window at a time regardless of provider, both to keep each request's
# token count reasonable and to keep individual LLM calls focused.
GROUP_WINDOW_SECONDS = 15 * 60
GROUP_WINDOW_OVERLAP_SECONDS = 90

# Cerebras's free tier caps at 5 requests/minute — tighter than its token
# budget for this workload by a wide margin, so pacing (not token size) is
# the binding constraint. 60/5=12s is the bare minimum; pad it so per-call
# latency/jitter can't push us over the boundary and trigger an avoidable 429.
CEREBRAS_REQUEST_DELAY_SECONDS = 13.0

LLM_MAX_RETRIES = 5
LLM_RETRY_BASE_DELAY = 2.0  # seconds; doubles each attempt, plus jitter

# A Retry-After above this is not a short burst limit worth silently
# sleeping through — it's almost always a daily/hourly token cap, which
# won't clear soon regardless of how many times we retry. Fail fast with a
# clear message instead of blocking an admin's request with zero feedback.
LLM_MAX_RETRY_DELAY = 90.0


class PipelineError(Exception):
    """Raised for any expected failure (bad link, ffmpeg/API error) so
    callers can show the admin a clean message instead of a raw traceback."""


def get_groq_client(groq_api_key: str) -> Groq:
    # max_retries=0: the SDK's own built-in retry-on-429 silently sleeps
    # *before* our exception ever reaches _call_llm_with_retry, which can
    # hide several minutes of retries/backoff behind what looks like one
    # slow call. We do our own retry/fail-fast on top, so disable it here.
    return Groq(api_key=groq_api_key, max_retries=0, timeout=120.0)


def get_cerebras_client(cerebras_api_key: str) -> OpenAI:
    return OpenAI(api_key=cerebras_api_key, base_url=CEREBRAS_BASE_URL, max_retries=0, timeout=60.0)


def _format_wait(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}ч{m}м"
    if m:
        return f"{m}м{s}с"
    return f"{s}с"


def _call_llm_with_retry(fn, status_error_cls, *, max_retries: int = LLM_MAX_RETRIES):
    """Call an LLM SDK function (Groq or Cerebras/OpenAI — both Stainless-
    generated clients with an identical APIStatusError shape), retrying on
    429 (rate limit) / 413 (payload too large) with exponential backoff —
    honoring a Retry-After header when the provider sends one. Any other
    error, or exhausting all retries, raises PipelineError so callers never
    see a raw SDK exception.

    Some quota errors (e.g. Groq's daily token cap) can't be fixed by a
    short retry — Groq flags those with `x-should-retry: false`; as a
    provider-agnostic fallback, any Retry-After above LLM_MAX_RETRY_DELAY is
    also treated as non-retryable. Silently sleeping through either would
    block an admin's request for a long time with zero feedback, so those
    fail immediately with a clear message instead.
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except status_error_cls as exc:
            if exc.status_code not in (429, 413):
                raise PipelineError(f"LLM API вернул ошибку {exc.status_code}: {exc}") from exc

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
                    f"LLM API: лимит запросов/токенов исчерпан{wait_msg} ({exc})"
                ) from exc

            last_exc = exc
            if attempt == max_retries:
                break
            delay = retry_after if retry_after is not None else LLM_RETRY_BASE_DELAY * (2 ** attempt)
            delay += random.uniform(0, 1)  # jitter, avoid retry bursts lining back up
            logger.warning(
                "LLM API вернула %s, повтор через %.1fс (попытка %d/%d)",
                exc.status_code, delay, attempt + 1, max_retries,
            )
            time.sleep(delay)
    raise PipelineError(
        f"LLM API: превышен лимит запросов после {max_retries} попыток "
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

    response = _call_llm_with_retry(_call, groq.APIStatusError)
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


def _extract_json_content(resp) -> str:
    """Reasoning models (gpt-oss-120b) can burn their whole max_tokens budget
    on hidden chain-of-thought and finish with an empty final answer even
    though the request itself succeeded — surface that as a clear
    PipelineError instead of a confusing json.loads(None) crash."""
    choice = resp.choices[0]
    content = choice.message.content
    if not content:
        raise PipelineError(
            f"LLM вернул пустой ответ (finish_reason={choice.finish_reason}) — "
            "вероятно, не хватило max_tokens на рассуждение"
        )
    return content


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


def _group_window(client: OpenAI, video_title: str, segments: list[dict]) -> list[dict]:
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
            max_tokens=2000,
            # gpt-oss-120b is a reasoning model — its hidden chain-of-thought
            # eats into the same max_tokens budget as the final answer.
            # "low" leaves enough room for the JSON reply on a task this
            # simple (default "medium" burned ~800 reasoning tokens per
            # window in testing, sometimes truncating the answer entirely).
            reasoning_effort="low",
        )

    try:
        resp = _call_llm_with_retry(_call, openai.APIStatusError)
        data = json.loads(_extract_json_content(resp))
    except PipelineError:
        raise
    except Exception as exc:
        raise PipelineError(f"LLM: не удалось сгруппировать темы ({exc})") from exc

    return _dedupe_ascending(_parse_topics_response(data))


def _collapse_duplicate_topics(client: OpenAI, video_title: str, topics: list[dict]) -> list[dict]:
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
            max_tokens=5000,
            reasoning_effort="low",
        )

    try:
        resp = _call_llm_with_retry(_call, openai.APIStatusError)
        data = json.loads(_extract_json_content(resp))
        collapsed = _dedupe_ascending(_parse_topics_response(data))
    except Exception as exc:
        logger.warning("Collapse pass failed (%s) — keeping un-collapsed topic list", exc)
        return topics

    return collapsed if collapsed else topics


def group_into_topics(video_title: str, segments: list[dict], cerebras_api_key: str) -> list[dict]:
    """LLM pass (Cerebras, gpt-oss-120b): turn a flat transcript into a topic
    outline with timecodes.

    Long transcripts are split into ~15-minute (real video time) overlapping
    windows and grouped one window at a time — both to keep each request's
    token count reasonable and to respect Cerebras's free-tier 5 requests/
    minute cap (the actual binding constraint here, well before its
    1,000,000 tokens/day budget). Results are merged by timecode and, if
    there was more than one window, passed through one more LLM call to
    collapse any topic that got split across a window boundary.

    Returns a list of {"title": str, "start_seconds": int}, strictly
    ascending by start_seconds, each start_seconds grounded in an actual
    segment start (the model is instructed to copy one, not invent one).
    """
    client = get_cerebras_client(cerebras_api_key)
    windows = _chunk_segments_by_time(segments, GROUP_WINDOW_SECONDS, GROUP_WINDOW_OVERLAP_SECONDS)
    if not windows:
        raise PipelineError("транскрипт пуст — нечего группировать")

    all_topics: list[dict] = []
    for i, window_segments in enumerate(windows):
        all_topics.extend(_group_window(client, video_title, window_segments))
        if i < len(windows) - 1:
            time.sleep(CEREBRAS_REQUEST_DELAY_SECONDS)

    merged = _dedupe_ascending(all_topics)
    if len(windows) > 1 and len(merged) > 1:
        time.sleep(CEREBRAS_REQUEST_DELAY_SECONDS)
        merged = _dedupe_ascending(_collapse_duplicate_topics(client, video_title, merged))

    if not merged:
        raise PipelineError("LLM не вернул ни одной темы")
    return merged


# Reasoning tokens eat into the same max_tokens budget as the JSON answer
# (see _extract_json_content) — a large draft like a 100+-topic live
# roadmap transcript needs enough headroom to echo the *entire* list back,
# not just the edited entries, so this is higher than _group_window's 2000.
EDIT_TOPICS_MAX_TOKENS = 8000

# gpt-oss-120b on Cerebras's free tier (what GROUPING_MODEL runs on here) caps
# input at ~65k tokens (paid tier: ~131k) - see
# https://inference-docs.cerebras.ai/models/openai-oss. There's no tokenizer
# dependency in this project, so characters-per-token is a rough proxy.
#
# 40_000 (the original value here) turned out not to be conservative enough:
# on the real ~3.5h "Дорожная карта" draft (109 topics, transcript downsampled
# to its cap), the full prompt measured ~97.9k chars. At the assumed 2.5
# chars/token that looked safely under the 65k cap (~39k tokens), but the
# request got a persistent 429 with an identical ~60s Retry-After on every
# one of 5 retries over ~5 minutes - a fixed cap being violated, not
# transient congestion (which would be expected to clear at least once).
# That implies the real tokenizer is less efficient for Cyrillic than 2.5
# chars/token (plausibly ~1.5-1.8), putting the actual request at or over
# 65k input tokens. Cut hard, with real margin against that uncertainty,
# rather than continuing to guess at the ratio.
TRANSCRIPT_CHARS_PER_TOKEN_ESTIMATE = 2.5
TRANSCRIPT_MAX_INPUT_TOKENS = 15_000
TRANSCRIPT_MAX_CHARS = int(TRANSCRIPT_MAX_INPUT_TOKENS * TRANSCRIPT_CHARS_PER_TOKEN_ESTIMATE)


def _fit_transcript_to_budget(lines: list[str], max_chars: int) -> tuple[list[str], bool]:
    """If `lines` joined would exceed max_chars, uniformly downsample (keep
    every Nth line) instead of truncating the tail - so the LLM still sees
    text spanning the whole video instead of just its first N minutes, which
    matters for instructions like "12 real steps" that need full coverage.
    Returns (possibly-thinned lines, whether thinning happened)."""
    total_chars = sum(len(line) + 1 for line in lines)
    if total_chars <= max_chars or len(lines) <= 1:
        return lines, False
    step = math.ceil(total_chars / max_chars)
    return lines[::step], True


def edit_topics_via_instruction(
    video_title: str,
    topics: list[dict],
    instruction: str,
    cerebras_api_key: str,
    transcript: list[dict] | None = None,
) -> dict:
    """One LLM call (Cerebras, gpt-oss-120b): apply a natural-language
    editing instruction to an existing topic list — merge, split, rename,
    delete, retime, or reorder. This is the "edit via chat" flow (Этап 2.1).

    `transcript`, if given, is the raw Whisper transcript saved by
    process_pending_lesson right after transcription (see
    db.save_pending_lesson_transcript) — a list of {"start_seconds"/"start",
    "text"} dicts in chronological order. Passing it lets the LLM ground its
    edits (splitting a topic, judging where a "real" step actually starts) in
    what was actually said instead of guessing from titles/timecodes alone,
    which is what made instructions like "split into 12 real steps, nothing
    extra" unreliable before. Omit it (None/empty) to fall back to the old
    topics-only behavior, where it can't ground a new/split topic's
    start_seconds in an actual transcript segment.

    `topics` and the returned list are both 1-indexed in the prompt text
    (not in the data itself) to match how the admin sees them numbered in
    Telegram (see _begin_edit_session in bot.py) — the admin's instruction
    ("тема 3", "объедини 5 и 6") only resolves to the right item if both
    sides agree on the same numbering.

    Returns {"topics": [{"title": str, "start_seconds": int}, ...], "summary": str}.
    Raises PipelineError if the LLM's response is malformed or the
    resulting topic list would be empty — callers must not save either.
    """
    client = get_cerebras_client(cerebras_api_key)
    listing = "\n".join(f"{i + 1}. [{t['start_seconds']}s] {t['title']}" for i, t in enumerate(topics))

    transcript_section = ""
    if transcript:
        lines = [
            f"[{int(seg.get('start_seconds', seg.get('start', 0)))}s] {seg['text']}" for seg in transcript
        ]
        fitted_lines, was_thinned = _fit_transcript_to_budget(lines, TRANSCRIPT_MAX_CHARS)
        note = ""
        if was_thinned:
            logger.warning(
                "Transcript for '%s' too large for edit_topics_via_instruction "
                "(%d chars, %d lines) - downsampled to %d lines to fit context budget",
                video_title, sum(len(l) for l in lines), len(lines), len(fitted_lines),
            )
            note = (
                " Ниже показана лишь часть реплик, равномерно взятая по всему видео (оно слишком "
                "длинное, чтобы влезло целиком) — ориентируйся по ним приблизительно, между "
                "показанными репликами могла быть речь, которую ты не видишь."
            )
        transcript_section = (
            "\n\nПолный транскрипт видео (таймкод и реплика) — используй его, чтобы понять, "
            "что реально происходит в видео, а не только заголовки текущих тем."
            + note + "\n\n" + "\n".join(fitted_lines) + "\n"
        )

    if transcript:
        transcript_note_system = (
            "У тебя есть доступ к транскрипту реальной речи из видео (см. ниже в сообщении) — "
            "используй его, чтобы находить реальные смысловые границы тем и точные start_seconds, "
            "а не только ориентироваться на текущие заголовки."
        )
    else:
        transcript_note_system = (
            "У тебя нет доступа к транскрипту видео — только к этому списку тем, поэтому при "
            "разбиении темы на несколько подбирай новые start_seconds на глаз где-то между старым "
            "start_seconds этой темы и следующей по порядку."
        )

    system = (
        "Ты — ассистент редактирования оглавления учебного видео через чат. "
        "Админ прислал пронумерованный список тем (заголовок + таймкод начала "
        "в секундах, темы пронумерованы с 1) и текстовую инструкцию на русском, "
        "как его изменить. Разрешённые операции: объединить темы, разбить тему "
        "на несколько, переименовать, удалить, изменить таймкод, поменять "
        f"порядок. {transcript_note_system} Отвечай только валидным JSON."
    )
    user = (
        f"Видео: «{video_title}»\n\n"
        f"Текущий список тем:\n\n{listing}\n"
        f"{transcript_section}\n"
        f"Инструкция администратора:\n{instruction}\n\n"
        "Примени инструкцию и верни ИТОГОВЫЙ список тем целиком (включая те, что "
        "не менялись), строго по возрастанию start_seconds, заголовки на русском. "
        "Если инструкцию невозможно выполнить (например, номер темы не существует) "
        "— верни список без изменений и объясни проблему в summary.\n\n"
        'Ответь строго в формате JSON: {"topics": [{"title": "...", "start_seconds": 0}], '
        '"summary": "краткое описание на русском, что именно изменилось (или почему не получилось)"}'
    )

    def _call():
        return client.chat.completions.create(
            model=GROUPING_MODEL,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            response_format={"type": "json_object"},
            temperature=0.2,
            max_tokens=EDIT_TOPICS_MAX_TOKENS,
            reasoning_effort="low",
        )

    try:
        resp = _call_llm_with_retry(_call, openai.APIStatusError)
        data = json.loads(_extract_json_content(resp))
    except PipelineError:
        raise
    except Exception as exc:
        raise PipelineError(f"LLM: не удалось применить правку ({exc})") from exc

    topics_out = _dedupe_ascending(_parse_topics_response(data))
    if not topics_out:
        raise PipelineError("LLM вернул пустой список тем — правка не применена")

    summary = data.get("summary") if isinstance(data, dict) else None
    summary = str(summary).strip()[:500] if summary else "Список тем обновлён."

    return {"topics": topics_out, "summary": summary}
