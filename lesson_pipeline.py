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
import math
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

import yt_dlp
from groq import Groq

CHUNK_MB = 22  # stay safely under Groq's 25 MB Whisper upload limit
WHISPER_MODEL = "whisper-large-v3"
GROUPING_MODEL = "llama-3.3-70b-versatile"


class PipelineError(Exception):
    """Raised for any expected failure (bad link, ffmpeg/API error) so
    callers can show the admin a clean message instead of a raw traceback."""


def get_groq_client(groq_api_key: str) -> Groq:
    return Groq(api_key=groq_api_key)


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
    with open(chunk_path, "rb") as f:
        response = client.audio.transcriptions.create(
            file=(chunk_path.name, f, "audio/mpeg"),
            model=WHISPER_MODEL,
            response_format="verbose_json",
            language="ru",
            timestamp_granularities=["segment"],
        )
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
            except Exception as exc:
                raise PipelineError(f"Whisper: ошибка транскрипции ({exc})") from exc
            all_segments.extend(segs)

    if not all_segments:
        raise PipelineError("транскрипция вернула пустой результат")
    return all_segments


def group_into_topics(video_title: str, segments: list[dict], groq_api_key: str) -> list[dict]:
    """LLM pass: turn a flat transcript into a topic outline with timecodes.

    Returns a list of {"title": str, "start_seconds": int}, strictly
    ascending by start_seconds, each start_seconds grounded in an actual
    segment start (the model is instructed to copy one, not invent one).
    """
    client = get_groq_client(groq_api_key)
    transcript = "\n".join(f"[{int(seg['start'])}s] {seg['text']}" for seg in segments)

    system = (
        "Ты помогаешь разбить транскрипт вебинара на осмысленные тематические блоки "
        "с таймкодами начала — как оглавление видео. Отвечай только валидным JSON."
    )
    user = (
        f"Видео: «{video_title}»\n\n"
        f"Транскрипт с таймкодами начала каждого сегмента речи:\n\n{transcript}\n\n"
        "Разбей видео на последовательные смысловые темы (обычно 8-25 тем для видео "
        "длительностью 1-3 часа). Для каждой темы дай короткий заголовок на русском "
        "(до 80 символов) и start_seconds — целое число секунд, совпадающее с "
        "таймкодом ПЕРВОГО сегмента этой темы из списка выше. Темы должны идти строго "
        "по возрастанию start_seconds, первая тема должна начинаться с 0 или близко к 0.\n\n"
        'Ответь строго в формате JSON: {"topics": [{"title": "...", "start_seconds": 0}]}'
    )

    try:
        resp = client.chat.completions.create(
            model=GROUPING_MODEL,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            response_format={"type": "json_object"},
            temperature=0.2,
            max_tokens=4000,
        )
        data = json.loads(resp.choices[0].message.content)
    except Exception as exc:
        raise PipelineError(f"LLM: не удалось сгруппировать темы ({exc})") from exc

    topics = data.get("topics") if isinstance(data, dict) else None
    if not isinstance(topics, list) or not topics:
        raise PipelineError("LLM не вернул ни одной темы")

    cleaned = []
    for t in topics:
        if not isinstance(t, dict):
            continue
        title = str(t.get("title", "")).strip()
        start = t.get("start_seconds")
        if not title or not isinstance(start, (int, float)):
            continue
        cleaned.append({"title": title[:200], "start_seconds": int(start)})
    cleaned.sort(key=lambda t: t["start_seconds"])

    result: list[dict] = []
    for t in cleaned:
        if result and t["start_seconds"] <= result[-1]["start_seconds"]:
            continue  # drop non-advancing/duplicate timecodes from a noisy LLM response
        result.append(t)

    if not result:
        raise PipelineError("LLM вернул темы в неверном формате")
    return result
