# ============================================================
# Colab yt-dlp downloader -> Google Drive uploader
# Downloads one video/media item with yt-dlp, then uploads it
# either as one file or as large .part#### files.
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

# Optional fixed output name without extension.
OUTPUT_BASENAME = ""

DRIVE_OUTPUT_DIR = "/content/drive/MyDrive/ColabYTDownloads"
LOCAL_WORK_DIR = "/content/yt_download_work"

MAX_SINGLE_FILE_BYTES = 10 * 1024**3
PART_SIZE_BYTES = int(9.5 * 1024**3)

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


def parse_part_selection(text, total_parts):
    text = text.strip().replace(" ", "")

    if not text:
        return list(range(1, total_parts + 1))

    selected = set()

    for token in text.split(","):
        if not token:
            continue

        if "-" in token:
            a, b = token.split("-", 1)
            start = int(a)
            end = int(b)

            if start > end:
                start, end = end, start

            for i in range(start, end + 1):
                selected.add(i)
        else:
            selected.add(int(token))

    bad = [i for i in selected if i < 1 or i > total_parts]
    if bad:
        raise ValueError(f"Invalid part number(s): {bad}. Valid range is 1-{total_parts}.")

    return sorted(selected)


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
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", "yt-dlp"],
        check=True,
    )


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
            "COOKIES_TEXT was provided, but it is neither Netscape cookie text "
            "nor a valid Cookie header string."
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
        p for p in fallback_dir.iterdir()
        if p.is_file() and not p.name.startswith(".") and p.suffix not in {".json", ".txt", ".part"}
    ]
    if not files:
        raise RuntimeError("yt-dlp finished but no downloaded file was found.")

    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
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


def upload_to_drive(local_path, drive_output_dir):
    local_path = Path(local_path)
    drive_output_dir = Path(drive_output_dir)
    drive_output_dir.mkdir(parents=True, exist_ok=True)

    drive_path = drive_output_dir / local_path.name

    if drive_path.exists():
        if not yes_no(f"Drive file already exists: {drive_path.name}. Overwrite?", default=False):
            print(f"Skipped upload because Drive file already exists: {drive_path}")
            return drive_path

        drive_path.unlink()

    print(f"\nUploading to Google Drive: {drive_path}")
    shutil.copy2(local_path, drive_path)
    os.sync()

    local_size = local_path.stat().st_size
    drive_size = drive_path.stat().st_size

    if drive_size != local_size:
        raise RuntimeError(
            f"Upload verification failed for {drive_path.name}: local={local_size}, drive={drive_size}"
        )

    print(f"Uploaded: {drive_path} ({human_size(drive_size)})")
    return drive_path


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


def write_and_upload_manifest(local_file, source_url, drive_output_dir):
    local_file = Path(local_file)
    size = local_file.stat().st_size
    total_parts = math.ceil(size / PART_SIZE_BYTES)

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

    manifest_path = Path(LOCAL_WORK_DIR) / f"{local_file.name}.manifest.json"

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    upload_to_drive(manifest_path, drive_output_dir)

    if DELETE_LOCAL_AFTER_UPLOAD:
        manifest_path.unlink(missing_ok=True)


def process_large_file_in_parts(local_file, source_url):
    local_file = Path(local_file)
    size = local_file.stat().st_size
    total_parts = math.ceil(size / PART_SIZE_BYTES)

    print("\nLarge file detected.")
    print(f"Downloaded file: {local_file.name}")
    print(f"Local size:      {human_size(size)}")
    print(f"Part size:       {human_size(PART_SIZE_BYTES)}")
    print(f"Total parts:     {total_parts}")
    print()
    print("Choose which part(s) to process this run.")
    print("Examples:")
    print("  Press Enter  -> all parts")
    print("  3            -> only part 3")
    print("  1,4,7        -> parts 1, 4, and 7")
    print("  2-5          -> parts 2 through 5")
    print()

    selection_text = input(f"Part selection [1-{total_parts}, blank=all]: ")
    selected_parts = parse_part_selection(selection_text, total_parts)

    print(f"\nSelected parts: {selected_parts}")

    for part_number in selected_parts:
        start = (part_number - 1) * PART_SIZE_BYTES
        end = min(start + PART_SIZE_BYTES - 1, size - 1)
        expected = end - start + 1

        part_name = f"{local_file.name}.part{part_number:04d}-of-{total_parts:04d}"
        local_part = Path(LOCAL_WORK_DIR) / part_name
        drive_part = Path(DRIVE_OUTPUT_DIR) / part_name

        print("\n" + "=" * 70)
        print(f"Part {part_number}/{total_parts}")
        print(f"Name:       {part_name}")
        print(f"Byte range: {start}-{end}")
        print(f"Size:       {human_size(expected)}")

        if drive_part.exists() and drive_part.stat().st_size == expected:
            if yes_no(f"Part already exists in Drive with the right size. Skip part {part_number}?", default=True):
                continue

        if not yes_no(f"Create and upload part {part_number} now?", default=True):
            print(f"Skipped part {part_number}.")
            continue

        write_local_part(local_file, start, end, local_part)

        if yes_no(f"Upload {part_name} to Google Drive now?", default=True):
            upload_to_drive(local_part, DRIVE_OUTPUT_DIR)

            if DELETE_LOCAL_AFTER_UPLOAD:
                local_part.unlink(missing_ok=True)
                print(f"Deleted local part to free space: {local_part}")
        else:
            print(f"Kept local part here: {local_part}")
            input("Press Enter after you have freed space, or stop the cell to quit.")

    print("\nWriting manifest file to Google Drive...")
    write_and_upload_manifest(local_file=local_file, source_url=source_url, drive_output_dir=DRIVE_OUTPUT_DIR)
    print("\nDone with selected large-file parts.")


def main():
    if MAX_SINGLE_FILE_BYTES <= 0:
        raise ValueError("MAX_SINGLE_FILE_BYTES must be greater than zero.")

    if PART_SIZE_BYTES <= 0:
        raise ValueError("PART_SIZE_BYTES must be greater than zero.")

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

        print(f"\nDrive folder:   {DRIVE_OUTPUT_DIR}")
        print(f"Local work dir: {LOCAL_WORK_DIR}")
        print(f"Quality:        {QUALITY}")
        print(f"Format selector:{build_format_selector()}")
        print(f"Local file:     {local_file}")
        print(f"Local size:     {human_size(size)}")

        if size <= MAX_SINGLE_FILE_BYTES:
            print("\nFile is within the single-file threshold.")
            upload_to_drive(local_file, DRIVE_OUTPUT_DIR)

            if DELETE_LOCAL_AFTER_UPLOAD:
                local_file.unlink(missing_ok=True)
                print(f"Deleted local file to free space: {local_file}")

        else:
            process_large_file_in_parts(local_file=local_file, source_url=URL)

            if DELETE_LOCAL_AFTER_UPLOAD and local_file.exists():
                if yes_no(f"Delete the original local yt-dlp file now? {local_file.name}", default=True):
                    local_file.unlink(missing_ok=True)
                    print(f"Deleted local file to free space: {local_file}")

    finally:
        if cookie_file is not None:
            cookie_file.unlink(missing_ok=True)


main()
