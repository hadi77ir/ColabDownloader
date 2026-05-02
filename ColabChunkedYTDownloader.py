# ============================================================
# Colab yt-dlp downloader -> Google Drive chunk batch uploader
# Downloads one video/media item with yt-dlp, then creates chunk
# files for one split batch and uploads that batch to Drive.
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


# ============================================================
# USER SETTINGS
# ============================================================

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

SPLIT_PART_INDEX = 1

DRIVE_OUTPUT_ROOT = "/content/drive/MyDrive/ColabChunkYTDownloads"
LOCAL_WORK_ROOT = "/content/yt_chunk_work"

CHUNK_SIZE_BYTES = 10_000_000
MAX_LOCAL_BATCH_BYTES = 10_000_000_000
CHUNKS_PER_SPLIT_PART = MAX_LOCAL_BATCH_BYTES // CHUNK_SIZE_BYTES

DELETE_LOCAL_CHUNKS_AFTER_UPLOAD = True
DELETE_LOCAL_BATCH_AFTER_CONFIRMATION = True
ASK_TO_DELETE_DRIVE_BATCH_AFTER_CONFIRMATION = False
DELETE_LOCAL_SOURCE_AFTER_LAST_SPLIT = False

MAX_RETRIES = 5
COPY_BUFFER_BYTES = 2 * 1024 * 1024


# ============================================================
# HELPERS
# ============================================================

def human_size(n):
    if n is None:
        return "unknown size"

    n = float(n)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]

    for unit in units:
        if n < 1000 or unit == units[-1]:
            return f"{n:.2f} {unit}"
        n /= 1000


def safe_filename(name):
    name = name.strip().replace("\x00", "")
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    name = re.sub(r"\s+", " ", name)

    if not name:
        name = "downloaded_media"

    if len(name) > 120:
        stem = Path(name).stem[:90]
        suffix = Path(name).suffix[:20]
        name = stem + suffix

    return name


def yes_no(prompt, default=True):
    suffix = "[Y/n]" if default else "[y/N]"
    ans = input(f"{prompt} {suffix} ").strip().lower()

    if not ans:
        return default

    return ans in {"y", "yes"}


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


def chunk_name(filename, global_chunk_index, total_chunks):
    return f"{filename}.chunk{global_chunk_index:08d}-of-{total_chunks:08d}"


def write_chunk_from_local_file(source_file, start, end, local_chunk):
    source_file = Path(source_file)
    local_chunk = Path(local_chunk)
    expected_size = end - start + 1

    if local_chunk.exists() and local_chunk.stat().st_size == expected_size:
        return "already-local"

    if local_chunk.exists():
        local_chunk.unlink()

    local_chunk.parent.mkdir(parents=True, exist_ok=True)

    with open(source_file, "rb") as src, open(local_chunk, "wb") as dst:
        src.seek(start)
        remaining = expected_size
        written = 0
        last_print = 0

        while remaining > 0:
            block = src.read(min(COPY_BUFFER_BYTES, remaining))
            if not block:
                raise RuntimeError(f"Unexpected EOF while creating {local_chunk.name}")

            dst.write(block)
            remaining -= len(block)
            written += len(block)

            now = time.time()
            if now - last_print > 1 or written == expected_size:
                print(
                    f"\rChunk build: {human_size(written)} / {human_size(expected_size)}",
                    end="",
                    flush=True,
                )
                last_print = now

    print()

    actual_size = local_chunk.stat().st_size
    if actual_size != expected_size:
        raise RuntimeError(
            f"Chunk size mismatch for {local_chunk.name}: got {actual_size}, expected {expected_size}"
        )

    return "created"


def copy_file_to_drive(local_path, drive_path):
    local_path = Path(local_path)
    drive_path = Path(drive_path)
    drive_path.parent.mkdir(parents=True, exist_ok=True)

    expected_size = local_path.stat().st_size

    if drive_path.exists() and drive_path.stat().st_size == expected_size:
        return "already-drive"

    if drive_path.exists():
        drive_path.unlink()

    shutil.copy2(local_path, drive_path)
    os.sync()

    actual_size = drive_path.stat().st_size
    if actual_size != expected_size:
        raise RuntimeError(
            f"Drive copy verification failed for {drive_path.name}: got {actual_size}, expected {expected_size}"
        )

    return "uploaded"


def write_manifest(
    manifest_path,
    original_filename,
    source_url,
    total_size,
    chunk_size,
    split_part_index,
    total_split_parts,
    chunks_per_split_part,
    start_chunk,
    end_chunk,
    total_chunks,
):
    manifest = {
        "original_filename": original_filename,
        "source_url": source_url,
        "total_size_bytes": total_size,
        "total_size_human": human_size(total_size),
        "chunk_size_bytes": chunk_size,
        "chunk_size_human": human_size(chunk_size),
        "chunks_per_split_part": chunks_per_split_part,
        "split_part_index": split_part_index,
        "total_split_parts": total_split_parts,
        "first_chunk_in_this_split_part": start_chunk,
        "last_chunk_in_this_split_part": end_chunk,
        "total_chunks": total_chunks,
        "quality": QUALITY,
        "yt_dlp_format": YT_DLP_FORMAT,
        "merge_output_format": MERGE_OUTPUT_FORMAT,
        "chunk_filename_pattern": f"{original_filename}.chunk########-of-{total_chunks:08d}",
        "rebuild_command_linux_macos": (
            f'cat "{original_filename}".chunk*-of-{total_chunks:08d} > "{original_filename}"'
        ),
        "notes": [
            "Chunk files are zero-padded, so alphabetical order is the rebuild order.",
            "Download all split folders from Google Drive before rebuilding.",
            "This yt-dlp workflow first downloads the complete media file into Colab before chunking it.",
        ],
    }

    manifest_path = Path(manifest_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return manifest_path


def remove_partial_files(folder):
    folder = Path(folder)
    for partial in folder.glob("*.partial"):
        partial.unlink(missing_ok=True)


# ============================================================
# MAIN
# ============================================================

def main():
    if SPLIT_PART_INDEX < 1:
        raise ValueError("SPLIT_PART_INDEX must be 1 or greater.")

    if CHUNK_SIZE_BYTES <= 0:
        raise ValueError("CHUNK_SIZE_BYTES must be greater than zero.")

    if MAX_LOCAL_BATCH_BYTES < CHUNK_SIZE_BYTES:
        raise ValueError("MAX_LOCAL_BATCH_BYTES must be at least CHUNK_SIZE_BYTES.")

    if CHUNKS_PER_SPLIT_PART < 1:
        raise ValueError("CHUNKS_PER_SPLIT_PART must be at least 1.")

    print("Mounting Google Drive...")
    drive.mount("/content/drive")

    Path(DRIVE_OUTPUT_ROOT).mkdir(parents=True, exist_ok=True)
    Path(LOCAL_WORK_ROOT).mkdir(parents=True, exist_ok=True)

    ensure_yt_dlp_installed()
    ensure_ffmpeg_installed()

    local_source_dir = Path(LOCAL_WORK_ROOT) / "_yt_source"
    local_source_dir.mkdir(parents=True, exist_ok=True)

    cookie_file = None

    try:
        cookie_file = write_cookie_file(
            url=URL,
            cookies_text=COOKIES_TEXT,
            output_path=Path(LOCAL_WORK_ROOT) / "_session" / ".yt_cookies.txt",
        )

        source_file = download_with_yt_dlp(URL, local_source_dir, cookie_file=cookie_file)
        total_size = source_file.stat().st_size
        total_chunks = math.ceil(total_size / CHUNK_SIZE_BYTES)
        total_split_parts = math.ceil(total_chunks / CHUNKS_PER_SPLIT_PART)

        start_chunk = (SPLIT_PART_INDEX - 1) * CHUNKS_PER_SPLIT_PART + 1
        end_chunk = min(SPLIT_PART_INDEX * CHUNKS_PER_SPLIT_PART, total_chunks)

        if start_chunk > total_chunks:
            print(
                f"\nNothing to upload. This file has only {total_split_parts} split part(s), "
                f"but SPLIT_PART_INDEX is {SPLIT_PART_INDEX}."
            )
            return

        start_byte = (start_chunk - 1) * CHUNK_SIZE_BYTES
        end_byte = min(end_chunk * CHUNK_SIZE_BYTES, total_size) - 1
        batch_size = end_byte - start_byte + 1
        batch_chunk_count = end_chunk - start_chunk + 1

        safe_base = safe_filename(source_file.name)
        local_split_dir = Path(LOCAL_WORK_ROOT) / safe_base / f"split_{SPLIT_PART_INDEX:04d}"
        drive_split_dir = Path(DRIVE_OUTPUT_ROOT) / safe_base / f"split_{SPLIT_PART_INDEX:04d}"

        local_split_dir.mkdir(parents=True, exist_ok=True)
        drive_split_dir.mkdir(parents=True, exist_ok=True)

        print(f"\nSource file:     {source_file}")
        print(f"Source size:     {human_size(total_size)}")
        print(f"Quality:         {QUALITY}")
        print(f"Format selector: {build_format_selector()}")
        print(f"Chunk size:      {human_size(CHUNK_SIZE_BYTES)}")
        print(f"Chunks/split:    {CHUNKS_PER_SPLIT_PART}")
        print(f"Split part:      {SPLIT_PART_INDEX}")
        print()
        print("Planned split part")
        print("------------------")
        print(f"Split part:       {SPLIT_PART_INDEX} of {total_split_parts}")
        print(f"Chunks:           {start_chunk} through {end_chunk}")
        print(f"Chunk count:      {batch_chunk_count}")
        print(f"Byte range:       {start_byte} through {end_byte}")
        print(f"Batch size:       {human_size(batch_size)}")
        print(f"Local folder:     {local_split_dir}")
        print(f"Drive folder:     {drive_split_dir}")

        if not yes_no("\nPrepare and upload this split part now?", default=True):
            print("Stopped before chunking.")
            return

        print("\nCreating chunk files and copying them to Google Drive...")

        uploaded = 0
        already = 0

        for global_chunk_index in range(start_chunk, end_chunk + 1):
            chunk_start = (global_chunk_index - 1) * CHUNK_SIZE_BYTES
            chunk_end = min(global_chunk_index * CHUNK_SIZE_BYTES, total_size) - 1
            expected = chunk_end - chunk_start + 1

            name = chunk_name(source_file.name, global_chunk_index, total_chunks)
            local_chunk = local_split_dir / name
            drive_chunk = drive_split_dir / name

            if drive_chunk.exists() and drive_chunk.stat().st_size == expected:
                already += 1

                if (
                    global_chunk_index == start_chunk
                    or global_chunk_index == end_chunk
                    or global_chunk_index % 25 == 0
                ):
                    done_in_batch = global_chunk_index - start_chunk + 1
                    print(
                        f"Chunk {global_chunk_index}/{total_chunks} "
                        f"({done_in_batch}/{batch_chunk_count}) already in Drive."
                    )

                continue

            status = write_chunk_from_local_file(
                source_file=source_file,
                start=chunk_start,
                end=chunk_end,
                local_chunk=local_chunk,
            )
            drive_status = copy_file_to_drive(local_chunk, drive_chunk)

            if drive_status == "uploaded":
                uploaded += 1
            else:
                already += 1

            if DELETE_LOCAL_CHUNKS_AFTER_UPLOAD:
                local_chunk.unlink(missing_ok=True)

            if (
                global_chunk_index == start_chunk
                or global_chunk_index == end_chunk
                or global_chunk_index % 25 == 0
            ):
                done_in_batch = global_chunk_index - start_chunk + 1
                print(
                    f"Chunk {global_chunk_index}/{total_chunks} "
                    f"({done_in_batch}/{batch_chunk_count}) {status}/{drive_status}: {name}"
                )

        remove_partial_files(local_split_dir)

        local_manifest = write_manifest(
            manifest_path=local_split_dir / f"{source_file.name}.split_{SPLIT_PART_INDEX:04d}.manifest.json",
            original_filename=source_file.name,
            source_url=URL,
            total_size=total_size,
            chunk_size=CHUNK_SIZE_BYTES,
            split_part_index=SPLIT_PART_INDEX,
            total_split_parts=total_split_parts,
            chunks_per_split_part=CHUNKS_PER_SPLIT_PART,
            start_chunk=start_chunk,
            end_chunk=end_chunk,
            total_chunks=total_chunks,
        )

        drive_manifest = drive_split_dir / local_manifest.name
        copy_file_to_drive(local_manifest, drive_manifest)

        print("\nThis split part is now in Google Drive.")
        print(f"Drive folder: {drive_split_dir}")
        print(f"Manifest:     {drive_manifest.name}")
        print(f"Uploaded:     {uploaded}")
        print(f"Already there:{already}")
        print()
        print("Download or verify this Drive folder now if you want a local copy.")
        print("After that, return to this Colab cell.")

        input("\nPress Enter after you have downloaded or verified this Drive split part... ")

        if DELETE_LOCAL_BATCH_AFTER_CONFIRMATION:
            if yes_no(f"Delete the local split folder at {local_split_dir}?", default=True):
                shutil.rmtree(local_split_dir, ignore_errors=True)
                print("Deleted local split folder.")

        if ASK_TO_DELETE_DRIVE_BATCH_AFTER_CONFIRMATION:
            if yes_no(
                f"Delete the Google Drive copy at {drive_split_dir}? "
                "Only do this if you already downloaded it elsewhere.",
                default=False,
            ):
                shutil.rmtree(drive_split_dir, ignore_errors=True)
                print("Deleted Google Drive split folder.")

        if SPLIT_PART_INDEX == total_split_parts and DELETE_LOCAL_SOURCE_AFTER_LAST_SPLIT:
            if yes_no(f"Delete the original local yt-dlp file at {source_file}?", default=True):
                source_file.unlink(missing_ok=True)
                print("Deleted local source file.")

        print("\nDone.")
        print(f"To continue, set SPLIT_PART_INDEX = {SPLIT_PART_INDEX + 1} and run again.")
        print(f"Total split parts for this file: {total_split_parts}")

    finally:
        if cookie_file is not None:
            cookie_file.unlink(missing_ok=True)


main()
