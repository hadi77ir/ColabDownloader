# ============================================================
# Colab yt-dlp downloader -> Google Drive uploader
# Downloads one media item with yt-dlp, uploads it to Drive,
# and for large files processes one part at a time in sequence.
# ============================================================

import json
import math
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from collections import deque
from http.cookies import SimpleCookie
from pathlib import Path
from urllib.parse import urlparse

from google.colab import drive


# -------------------------
# EDIT THESE SETTINGS
# -------------------------

URL = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"

COOKIES_TEXT = """
"""

# Leave as None for the best available quality.
# Set to 360, 480, 720, 1080, 1440, 2160, ... to cap the video height.
QUALITY = None

# Optional raw yt-dlp format selector.
# If this is not blank, it takes priority over QUALITY.
YT_DLP_FORMAT = ""

MERGE_OUTPUT_FORMAT = "mp4"
OUTPUT_BASENAME = ""

DRIVE_OUTPUT_DIR = "/content/drive/MyDrive/ColabYTDownloads"
LOCAL_WORK_DIR = "/content/yt_download_work"

MAX_SINGLE_FILE_BYTES = 10 * 1024**3
PART_SIZE_BYTES = int(9.5 * 1024**3)

# For large files, start from this part number and continue automatically.
START_PART_INDEX = 1

# yt-dlp uses concurrent fragment downloads when the source supports it.
YT_DLP_CONCURRENT_FRAGMENTS = 4

DELETE_LOCAL_AFTER_UPLOAD = True
MAX_RETRIES = 5
COPY_BUFFER_BYTES = 8 * 1024 * 1024


# -------------------------
# HELPERS
# -------------------------

def human_size(n):
    if n is None:
        return "unknown size"

    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    n = float(n)

    for unit in units:
        if n < 1024 or unit == units[-1]:
            return f"{n:.2f} {unit}"
        n /= 1024


def safe_filename(name):
    name = name.strip().replace("\x00", "")
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    name = re.sub(r"\s+", " ", name)
    return name or "downloaded_media"


def yes_no(prompt, default=True):
    suffix = "[Y/n]" if default else "[y/N]"
    ans = input(f"{prompt} {suffix} ").strip().lower()

    if not ans:
        return default

    return ans in {"y", "yes"}


def print_progress(label, done, total):
    if total:
        pct = done * 100 / total
        print(
            f"\r{label}: {human_size(done)} / {human_size(total)} ({pct:.1f}%)",
            end="",
            flush=True,
        )
    else:
        print(f"\r{label}: {human_size(done)}", end="", flush=True)


def ensure_yt_dlp_installed():
    try:
        import yt_dlp  # noqa: F401
        return
    except ImportError:
        pass

    print("Installing yt-dlp...")
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "yt-dlp"], check=True)


def ensure_ffmpeg_installed():
    if shutil.which("ffmpeg"):
        return

    print("Installing ffmpeg...")
    subprocess.run(["apt-get", "update", "-qq"], check=True)
    subprocess.run(["apt-get", "install", "-y", "-qq", "ffmpeg"], check=True)


def build_format_selector():
    explicit = str(YT_DLP_FORMAT or "").strip()
    if explicit:
        return explicit

    if QUALITY is None or str(QUALITY).strip() == "":
        return "bestvideo*+bestaudio/best"

    quality_text = str(QUALITY).strip().lower()

    if quality_text in {"best", "source"}:
        return "bestvideo*+bestaudio/best"

    if quality_text == "worst":
        return "worstvideo*+worstaudio/worst"

    if quality_text.isdigit():
        limit = int(quality_text)
        return f"bestvideo*[height<={limit}]+bestaudio/best[height<={limit}]/best"

    return quality_text


def write_cookie_file(url, cookies_text, output_path):
    text = (cookies_text or "").strip()
    if not text:
        return None

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if "\t" in text or text.startswith("# Netscape HTTP Cookie File"):
        if not text.startswith("# Netscape HTTP Cookie File"):
            text = "# Netscape HTTP Cookie File\n" + text

        output_path.write_text(text.rstrip() + "\n", encoding="utf-8")
        return output_path

    cookie = SimpleCookie()
    cookie.load(text)

    if not cookie:
        raise ValueError(
            "COOKIES_TEXT was provided, but it is neither Netscape cookie text nor a valid Cookie header string."
        )

    host = urlparse(url).hostname or "example.com"
    lines = ["# Netscape HTTP Cookie File"]

    for morsel in cookie.values():
        path = morsel["path"] or "/"
        secure = "TRUE" if morsel["secure"] else "FALSE"
        lines.append("\t".join([host, "FALSE", path, secure, "0", morsel.key, morsel.value]))

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


def find_downloaded_file(recent_lines, fallback_dir):
    for line in reversed(list(recent_lines)):
        candidate = Path(line.strip())
        if candidate.exists() and candidate.is_file() and not candidate.name.startswith("."):
            return candidate

    fallback_dir = Path(fallback_dir)
    files = [
        path
        for path in fallback_dir.iterdir()
        if path.is_file() and not path.name.startswith(".") and path.suffix not in {".json", ".txt", ".part"}
    ]
    if not files:
        raise RuntimeError("yt-dlp finished but no downloaded file was found.")

    files.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return files[0]


def download_with_yt_dlp(url, work_dir, cookie_file=None):
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    if OUTPUT_BASENAME.strip():
        output_template = str(work_dir / f"{safe_filename(OUTPUT_BASENAME)}.%(ext)s")
    else:
        output_template = str(work_dir / "%(title).180B [%(id)s].%(ext)s")

    command = [
        sys.executable,
        "-m",
        "yt_dlp",
        "--no-playlist",
        "--newline",
        "--continue",
        "--no-overwrites",
        "--restrict-filenames",
        "--retries",
        str(MAX_RETRIES),
        "--fragment-retries",
        str(MAX_RETRIES),
        "--merge-output-format",
        MERGE_OUTPUT_FORMAT,
        "--print",
        "before_dl:filepath",
        "--print",
        "after_move:filepath",
        "-f",
        build_format_selector(),
        "-o",
        output_template,
    ]

    if YT_DLP_CONCURRENT_FRAGMENTS > 1:
        command.extend(["-N", str(YT_DLP_CONCURRENT_FRAGMENTS)])

    if cookie_file is not None:
        command.extend(["--cookies", str(cookie_file)])

    command.append(url)

    print("\nRunning yt-dlp...")
    print(" ".join(shlex.quote(part) for part in command))

    recent_lines = deque(maxlen=400)

    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as process:
        assert process.stdout is not None

        for raw_line in process.stdout:
            line = raw_line.rstrip()
            if line:
                print(line)
                recent_lines.append(line)

        return_code = process.wait()

    if return_code != 0:
        raise RuntimeError(f"yt-dlp failed with exit code {return_code}.")

    local_file = find_downloaded_file(recent_lines, work_dir)
    print(f"\nDownloaded file: {local_file}")
    print(f"Downloaded size: {human_size(local_file.stat().st_size)}")
    return local_file


def remove_path(path):
    path = Path(path)
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    else:
        path.unlink(missing_ok=True)


def copy_file_to_drive(local_path, drive_path):
    local_path = Path(local_path)
    drive_path = Path(drive_path)
    drive_path.parent.mkdir(parents=True, exist_ok=True)

    expected_size = local_path.stat().st_size

    if drive_path.exists() and drive_path.stat().st_size == expected_size:
        return "already-drive", drive_path

    if drive_path.exists():
        drive_path.unlink()

    print(f"\nUploading to Google Drive: {drive_path}")
    shutil.copy2(local_path, drive_path)
    os.sync()

    actual_size = drive_path.stat().st_size
    if actual_size != expected_size:
        raise RuntimeError(
            f"Upload verification failed for {drive_path.name}: got {actual_size}, expected {expected_size}"
        )

    print(f"Uploaded: {drive_path} ({human_size(actual_size)})")
    return "uploaded", drive_path


def write_local_part(source_file, start, end, local_part):
    source_file = Path(source_file)
    local_part = Path(local_part)
    expected = end - start + 1

    if local_part.exists() and local_part.stat().st_size == expected:
        print(f"\nLocal part already complete: {local_part.name}")
        return local_part

    if local_part.exists():
        local_part.unlink()

    local_part.parent.mkdir(parents=True, exist_ok=True)

    print(f"\nCreating local part: {local_part.name}")

    with open(source_file, "rb") as src, open(local_part, "wb") as dst:
        src.seek(start)
        remaining = expected
        written = 0
        last_print = 0

        while remaining > 0:
            block = src.read(min(COPY_BUFFER_BYTES, remaining))
            if not block:
                raise RuntimeError(f"Unexpected EOF while creating {local_part.name}")

            dst.write(block)
            remaining -= len(block)
            written += len(block)

            now = time.time()
            if now - last_print > 1 or written == expected:
                print_progress("Part copy", written, expected)
                last_print = now

    print()

    actual = local_part.stat().st_size
    if actual != expected:
        raise RuntimeError(f"Local part size mismatch: got {actual}, expected {expected}")

    return local_part


def write_manifest(local_dir, local_file, source_url, total_parts):
    local_file = Path(local_file)
    size = local_file.stat().st_size

    manifest = {
        "original_filename": local_file.name,
        "source_url": source_url,
        "downloaded_size_bytes": size,
        "downloaded_size_human": human_size(size),
        "part_size_bytes": PART_SIZE_BYTES,
        "part_size_human": human_size(PART_SIZE_BYTES),
        "total_parts": total_parts,
        "quality": QUALITY,
        "yt_dlp_format": YT_DLP_FORMAT,
        "merge_output_format": MERGE_OUTPUT_FORMAT,
        "part_name_pattern": f"{local_file.name}.part0001-of-{total_parts:04d}",
        "rebuild_order": [
            f"{local_file.name}.part{i:04d}-of-{total_parts:04d}"
            for i in range(1, total_parts + 1)
        ],
    }

    manifest_path = Path(local_dir) / f"{local_file.name}.manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as file_obj:
        json.dump(manifest, file_obj, indent=2)

    return manifest_path


def handle_drive_cleanup(drive_paths, label, has_next_part):
    if not yes_no(f"Did you download {label}? I want to remove it from Drive.", default=False):
        print("Keeping the current Drive copy. Stopping here.")
        return False

    for drive_path in drive_paths:
        remove_path(drive_path)

    print("Removed the current Drive copy.")

    if not has_next_part:
        print("No more parts remain.")
        return False

    return yes_no("Want to download next part?", default=True)


def process_large_file_in_parts(local_file, source_url):
    local_file = Path(local_file)
    size = local_file.stat().st_size
    total_parts = math.ceil(size / PART_SIZE_BYTES)
    current_part = max(1, START_PART_INDEX)

    print("\nLarge file detected.")
    print(f"Downloaded file: {local_file.name}")
    print(f"Local size:      {human_size(size)}")
    print(f"Part size:       {human_size(PART_SIZE_BYTES)}")
    print(f"Total parts:     {total_parts}")
    print(f"Start part:      {current_part}")

    completed_all_parts = True

    while current_part <= total_parts:
        start = (current_part - 1) * PART_SIZE_BYTES
        end = min(start + PART_SIZE_BYTES - 1, size - 1)
        expected = end - start + 1

        part_name = f"{local_file.name}.part{current_part:04d}-of-{total_parts:04d}"
        local_part = Path(LOCAL_WORK_DIR) / part_name
        drive_part = Path(DRIVE_OUTPUT_DIR) / part_name

        print("\n" + "=" * 70)
        print(f"Part {current_part}/{total_parts}")
        print(f"Name:       {part_name}")
        print(f"Byte range: {start}-{end}")
        print(f"Size:       {human_size(expected)}")

        if drive_part.exists() and drive_part.stat().st_size == expected:
            print("Drive already has this part with the expected size.")
        else:
            write_local_part(local_file, start, end, local_part)
            copy_file_to_drive(local_part, drive_part)

        local_manifest = write_manifest(LOCAL_WORK_DIR, local_file, source_url, total_parts)
        drive_manifest = Path(DRIVE_OUTPUT_DIR) / local_manifest.name
        copy_file_to_drive(local_manifest, drive_manifest)

        if DELETE_LOCAL_AFTER_UPLOAD:
            remove_path(local_part)
            remove_path(local_manifest)
            print("Removed local temporary files for this part.")

        should_continue = handle_drive_cleanup(
            drive_paths=[drive_part, drive_manifest],
            label=f"Drive part {current_part}/{total_parts} and its manifest",
            has_next_part=current_part < total_parts,
        )

        if not should_continue:
            completed_all_parts = False
            break

        current_part += 1

    return completed_all_parts


def process_single_file(local_file):
    local_file = Path(local_file)
    drive_file = Path(DRIVE_OUTPUT_DIR) / local_file.name

    copy_file_to_drive(local_file, drive_file)

    if DELETE_LOCAL_AFTER_UPLOAD:
        remove_path(local_file)
        print("Removed local temporary files.")

    handle_drive_cleanup(
        drive_paths=[drive_file],
        label=f"the Drive file {local_file.name}",
        has_next_part=False,
    )


def main():
    if MAX_SINGLE_FILE_BYTES <= 0:
        raise ValueError("MAX_SINGLE_FILE_BYTES must be greater than zero.")

    if PART_SIZE_BYTES <= 0:
        raise ValueError("PART_SIZE_BYTES must be greater than zero.")

    if START_PART_INDEX < 1:
        raise ValueError("START_PART_INDEX must be 1 or greater.")

    if YT_DLP_CONCURRENT_FRAGMENTS < 1:
        raise ValueError("YT_DLP_CONCURRENT_FRAGMENTS must be 1 or greater.")

    print("Mounting Google Drive...")
    drive.mount("/content/drive")

    Path(DRIVE_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    Path(LOCAL_WORK_DIR).mkdir(parents=True, exist_ok=True)

    ensure_yt_dlp_installed()
    ensure_ffmpeg_installed()

    local_download_dir = Path(LOCAL_WORK_DIR) / "_yt_source"
    local_download_dir.mkdir(parents=True, exist_ok=True)

    cookie_file = None

    try:
        cookie_file = write_cookie_file(
            url=URL,
            cookies_text=COOKIES_TEXT,
            output_path=Path(LOCAL_WORK_DIR) / "_session" / ".yt_cookies.txt",
        )

        local_file = download_with_yt_dlp(URL, local_download_dir, cookie_file=cookie_file)
        size = local_file.stat().st_size

        print(f"\nDrive folder:         {DRIVE_OUTPUT_DIR}")
        print(f"Local work dir:       {LOCAL_WORK_DIR}")
        print(f"Quality:              {QUALITY}")
        print(f"Format selector:      {build_format_selector()}")
        print(f"Concurrent fragments: {YT_DLP_CONCURRENT_FRAGMENTS}")
        print(f"Local file:           {local_file}")
        print(f"Local size:           {human_size(size)}")

        if size <= MAX_SINGLE_FILE_BYTES:
            process_single_file(local_file)
        else:
            completed_all_parts = process_large_file_in_parts(local_file=local_file, source_url=URL)

            if completed_all_parts and DELETE_LOCAL_AFTER_UPLOAD and local_file.exists():
                remove_path(local_file)
                print("Removed the original local yt-dlp file.")

    finally:
        if cookie_file is not None:
            remove_path(cookie_file)


main()
