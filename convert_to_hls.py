"""Convert the 6 day-videos into HLS (10s segments, stream-copy, no re-encode).

Source videos are H.264/AAC with keyframes exactly every 10s, so `-c copy`
produces clean ~10s segments without quality loss or re-encoding time.
"""
import subprocess
import sys
from pathlib import Path

SRC_DIR = Path(r"D:\Высоцкий\Полные видео")
OUT_DIR = Path(r"C:\bos-bot\hls_output")

DAYS = {
    "day1": "День 1. Пять уровней эволюции бизнеса от ручного управления до системной компании. План перехода на следующий уровень..mp4",
    "day2": "День-2.mp4",
    "day3": "День-3.mp4",
    "day4": "День-4.mp4",
    "day5": "День 5. Система управления финансами которая дает спокойствие владельцу и устойчивый рост бизнеса. (1).mp4",
    "day6": "День 6. Как внедрять изменения в компанию для системного роста..mp4",
}


def convert(day, filename):
    src = SRC_DIR / filename
    out = OUT_DIR / day
    out.mkdir(parents=True, exist_ok=True)
    playlist = out / "playlist.m3u8"
    cmd = [
        "ffmpeg", "-y", "-i", str(src),
        "-c", "copy", "-map", "0",
        "-start_number", "0",
        "-hls_time", "10",
        "-hls_playlist_type", "vod",
        "-hls_segment_filename", str(out / "segment_%05d.ts"),
        str(playlist),
    ]
    print(f"\n=== {day}: {filename} ===")
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if r.returncode != 0:
        print(r.stderr[-3000:])
        raise SystemExit(f"ffmpeg failed for {day}")
    n_segments = len(list(out.glob("segment_*.ts")))
    print(f"{day}: OK, {n_segments} segments -> {playlist}")


if __name__ == "__main__":
    only = sys.argv[1:] or list(DAYS)
    for day in only:
        convert(day, DAYS[day])
    print("\nDone.")
