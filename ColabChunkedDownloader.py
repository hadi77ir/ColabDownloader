# ============================================================
# Colab URL downloader -> Google Drive chunk batch uploader
#
# Behavior:
#   - Downloads a remote file as many small chunk files.
#   - One "split part" = up to 10 GB of chunks.
#   - With defaults:
#       CHUNK_SIZE_BYTES = 10 MB
#       MAX_LOCAL_BATCH_BYTES = 10 GB
#       CHUNKS_PER_SPLIT_PART = 1000
#
# Usage:
#   1. Set URL.
#   2. Set SPLIT_PART_INDEX = 1.
#   3. Run the cell.
#   4. Download the created Google Drive folder.
#   5. Set SPLIT_PART_INDEX = 2.
#   6. Run again.
#   7. Repeat until all split parts are downloaded.
# ============================================================

import os
import re
import json
import math
import time
import shutil
from pathlib import Path
from urllib.parse import urlparse, unquote

import requests
from requests.structures import CaseInsensitiveDict
from google.colab import drive


# ============================================================
# USER SETTINGS
# ============================================================

URL = "https://huggingface.co/Youssofal/Qwen3.6-35B-A3B-Abliterated-Heretic-GGUF/resolve/main/Qwen3.6-35B-A3B-Abliterated-Heretic-Q4_K_M/Qwen3.6-35B-A3B-Abliterated-Heretic-Q4_K_M.gguf?download=true"

# Change this and rerun:
#   1 = first 1000 chunks
#   2 = second 1000 chunks
#   3 = third 1000 chunks
#   ...
SPLIT_PART_INDEX = 1

DRIVE_OUTPUT_ROOT = "/content/drive/MyDrive/ColabChunkDownloads"
LOCAL_WORK_ROOT = "/content/download_chunk_work"

# Decimal units, matching your example:
#   10 MB chunks * 1000 chunks = 10 GB
CHUNK_SIZE_BYTES = 10_000_000
MAX_LOCAL_BATCH_BYTES = 10_000_000_000

# With the defaults, this equals 1000.
CHUNKS_PER_SPLIT_PART = MAX_LOCAL_BATCH_BYTES // CHUNK_SIZE_BYTES

# Delete the local /content copy after you confirm the Drive copy exists.
DELETE_LOCAL_BATCH_AFTER_CONFIRMATION = True

# Usually keep this False.
# Turn True only if you want the script to ask whether to delete the Drive folder
# after you have downloaded that split part elsewhere.
ASK_TO_DELETE_DRIVE_BATCH_AFTER_CONFIRMATION = False

MAX_RETRIES = 5
REQUEST_STREAM_BYTES = 2 * 1024 * 1024

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 ColabChunkDownloader/1.0",
    "Accept-Encoding": "identity",
}


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
        name = "downloaded_file"

    # Avoid very long chunk filenames.
    if len(name) > 120:
        stem = Path(name).stem[:90]
        suffix = Path(name).suffix[:20]
        name = stem + suffix

    return name


def filename_from_headers_or_url(headers, url):
    cd = headers.get("content-disposition", "")

    m = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", cd, flags=re.I)
    if m:
        return safe_filename(unquote(m.group(1)))

    m = re.search(r'filename\s*=\s*"?([^";]+)"?', cd, flags=re.I)
    if m:
        return safe_filename(unquote(m.group(1)))

    path_name = Path(urlparse(url).path).name
    if path_name:
        return safe_filename(unquote(path_name))

    return "downloaded_file"


def yes_no(prompt, default=True):
    suffix = "[Y/n]" if default else "[y/N]"
    ans = input(f"{prompt} {suffix} ").strip().lower()

    if not ans:
        return default

    return ans in {"y", "yes"}


def get_remote_info(session, url):
    """
    Gets:
      - final redirected URL
      - filename
      - total size
      - whether HTTP Range requests work

    HTTP Range support is required so SPLIT_PART_INDEX = 2 can start from the
    second 10 GB section without redownloading the first one.
    """
    headers = CaseInsensitiveDict()
    final_url = url
    size = None
    range_supported = False

    try:
        r = session.head(
            url,
            headers=REQUEST_HEADERS,
            allow_redirects=True,
            timeout=30,
        )
        final_url = r.url
        headers.update(r.headers)

        cl = r.headers.get("Content-Length")
        if cl and cl.isdigit():
            size = int(cl)

    except requests.RequestException as exc:
        print(f"HEAD request failed; continuing with Range check: {exc}")

    range_headers = dict(REQUEST_HEADERS)
    range_headers["Range"] = "bytes=0-0"

    try:
        r = session.get(
            final_url,
            headers=range_headers,
            stream=True,
            allow_redirects=True,
            timeout=30,
        )

        final_url = r.url
        headers.update(r.headers)

        if r.status_code == 206:
            range_supported = True

            # Example: Content-Range: bytes 0-0/123456789
            content_range = r.headers.get("Content-Range", "")
            m = re.search(r"/(\d+)$", content_range)
            if m:
                size = int(m.group(1))

        r.close()

    except requests.RequestException as exc:
        print(f"Range check failed: {exc}")

    return {
        "final_url": final_url,
        "filename": filename_from_headers_or_url(headers, final_url),
        "size": size,
        "range_supported": range_supported,
    }


def chunk_name(filename, global_chunk_index, total_chunks):
    return f"{filename}.chunk{global_chunk_index:08d}-of-{total_chunks:08d}"


def download_range_to_file(session, url, start, end, local_path):
    """
    Downloads byte range [start, end] into one chunk file.
    Resumes from a .partial file if present.
    """
    local_path = Path(local_path)
    tmp_path = Path(str(local_path) + ".partial")
    expected_size = end - start + 1

    if local_path.exists() and local_path.stat().st_size == expected_size:
        return "already-local"

    if local_path.exists():
        print(f"Deleting wrong-size local chunk: {local_path.name}")
        local_path.unlink()

    local_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, MAX_RETRIES + 1):
        have = tmp_path.stat().st_size if tmp_path.exists() else 0

        if have > expected_size:
            tmp_path.unlink()
            have = 0

        if have == expected_size:
            tmp_path.rename(local_path)
            return "completed-from-partial"

        range_start = start + have
        headers = dict(REQUEST_HEADERS)
        headers["Range"] = f"bytes={range_start}-{end}"

        try:
            with session.get(
                url,
                headers=headers,
                stream=True,
                allow_redirects=True,
                timeout=(30, 120),
            ) as r:
                if r.status_code != 206:
                    raise RuntimeError(
                        f"Expected HTTP 206 Partial Content, got HTTP {r.status_code}. "
                        "This URL may not support byte-range downloads."
                    )

                with open(tmp_path, "ab") as f:
                    for block in r.iter_content(chunk_size=REQUEST_STREAM_BYTES):
                        if block:
                            f.write(block)

            if tmp_path.stat().st_size == expected_size:
                tmp_path.rename(local_path)
                return "downloaded"

            raise RuntimeError(
                f"Incomplete chunk: got {tmp_path.stat().st_size}, "
                f"expected {expected_size}"
            )

        except Exception as exc:
            print(f"Chunk download attempt {attempt}/{MAX_RETRIES} failed: {exc}")

            if attempt == MAX_RETRIES:
                raise

            time.sleep(min(30, 2 ** attempt))

    raise RuntimeError(f"Could not download {local_path.name}")


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
            f"Drive copy verification failed for {drive_path.name}: "
            f"got {actual_size}, expected {expected_size}"
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
        "chunk_filename_pattern": f"{original_filename}.chunk########-of-{total_chunks:08d}",
        "rebuild_command_linux_macos": (
            f'cat "{original_filename}".chunk*-of-{total_chunks:08d} > "{original_filename}"'
        ),
        "notes": [
            "Chunk files are zero-padded, so alphabetical order is the rebuild order.",
            "Download all split folders from Google Drive before rebuilding.",
            "Do not rename chunk files unless you preserve their ordering.",
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

    session = requests.Session()

    print("\nChecking remote file...")
    info = get_remote_info(session, URL)

    final_url = info["final_url"]
    filename = info["filename"]
    total_size = info["size"]
    range_supported = info["range_supported"]

    print(f"Filename:      {filename}")
    print(f"Final URL:     {final_url}")
    print(f"Remote size:   {human_size(total_size)}")
    print(f"Range support: {range_supported}")
    print(f"Chunk size:    {human_size(CHUNK_SIZE_BYTES)}")
    print(f"Chunks/split:  {CHUNKS_PER_SPLIT_PART}")
    print(f"Split part:    {SPLIT_PART_INDEX}")

    if total_size is None:
        raise RuntimeError(
            "The remote file size is unknown. This workflow needs Content-Length "
            "or Content-Range so it can calculate chunk numbers."
        )

    if not range_supported:
        raise RuntimeError(
            "This URL does not appear to support HTTP Range requests. "
            "Range support is required so SPLIT_PART_INDEX = 2 can start at the "
            "second 10 GB batch without downloading the first batch again."
        )

    total_chunks = math.ceil(total_size / CHUNK_SIZE_BYTES)
    total_split_parts = math.ceil(total_chunks / CHUNKS_PER_SPLIT_PART)

    start_chunk = (SPLIT_PART_INDEX - 1) * CHUNKS_PER_SPLIT_PART + 1
    end_chunk = min(SPLIT_PART_INDEX * CHUNKS_PER_SPLIT_PART, total_chunks)

    if start_chunk > total_chunks:
        print(
            f"\nNothing to download. This file has only {total_split_parts} "
            f"split part(s), but SPLIT_PART_INDEX is {SPLIT_PART_INDEX}."
        )
        return

    start_byte = (start_chunk - 1) * CHUNK_SIZE_BYTES
    end_byte = min(end_chunk * CHUNK_SIZE_BYTES, total_size) - 1
    batch_size = end_byte - start_byte + 1
    batch_chunk_count = end_chunk - start_chunk + 1

    safe_base = safe_filename(filename)

    local_split_dir = (
        Path(LOCAL_WORK_ROOT)
        / safe_base
        / f"split_{SPLIT_PART_INDEX:04d}"
    )

    drive_split_dir = (
        Path(DRIVE_OUTPUT_ROOT)
        / safe_base
        / f"split_{SPLIT_PART_INDEX:04d}"
    )

    local_split_dir.mkdir(parents=True, exist_ok=True)
    drive_split_dir.mkdir(parents=True, exist_ok=True)

    print("\nPlanned split part")
    print("------------------")
    print(f"Split part:       {SPLIT_PART_INDEX} of {total_split_parts}")
    print(f"Chunks:           {start_chunk} through {end_chunk}")
    print(f"Chunk count:      {batch_chunk_count}")
    print(f"Byte range:       {start_byte} through {end_byte}")
    print(f"Batch size:       {human_size(batch_size)}")
    print(f"Local folder:     {local_split_dir}")
    print(f"Drive folder:     {drive_split_dir}")

    if not yes_no("\nDownload this split part now?", default=True):
        print("Stopped before downloading.")
        return

    print("\nDownloading chunks into the Colab filesystem...")
    print("With the default settings, this batch is at most 10 GB.")

    for global_chunk_index in range(start_chunk, end_chunk + 1):
        chunk_start = (global_chunk_index - 1) * CHUNK_SIZE_BYTES
        chunk_end = min(global_chunk_index * CHUNK_SIZE_BYTES, total_size) - 1
        expected = chunk_end - chunk_start + 1

        name = chunk_name(filename, global_chunk_index, total_chunks)
        local_chunk = local_split_dir / name
        drive_chunk = drive_split_dir / name

        # If this chunk is already in Drive with the right size, skip it.
        if drive_chunk.exists() and drive_chunk.stat().st_size == expected:
            if global_chunk_index == start_chunk or global_chunk_index % 25 == 0:
                print(f"Drive already has chunk {global_chunk_index}/{total_chunks}; skipping.")
            continue

        status = download_range_to_file(
            session=session,
            url=final_url,
            start=chunk_start,
            end=chunk_end,
            local_path=local_chunk,
        )

        if (
            global_chunk_index == start_chunk
            or global_chunk_index == end_chunk
            or global_chunk_index % 25 == 0
        ):
            done_in_batch = global_chunk_index - start_chunk + 1
            print(
                f"Chunk {global_chunk_index}/{total_chunks} "
                f"({done_in_batch}/{batch_chunk_count}) {status}: "
                f"{local_chunk.name}"
            )

    remove_partial_files(local_split_dir)

    local_chunks = sorted(
        local_split_dir.glob(f"{filename}.chunk*-of-{total_chunks:08d}")
    )
    local_bytes = sum(p.stat().st_size for p in local_chunks)

    print("\nFinished local download phase.")
    print(f"Local chunks present: {len(local_chunks)}")
    print(f"Local bytes present:  {human_size(local_bytes)}")
    print(f"Local folder:         {local_split_dir}")

    if not yes_no("\nUpload/copy these local chunks to Google Drive now?", default=True):
        print(f"Stopped. Local chunks are still here: {local_split_dir}")
        return

    print("\nCopying chunks to Google Drive...")
    uploaded = 0
    already = 0

    for i, local_chunk in enumerate(local_chunks, start=1):
        drive_chunk = drive_split_dir / local_chunk.name
        status = copy_file_to_drive(local_chunk, drive_chunk)

        if status == "uploaded":
            uploaded += 1
        else:
            already += 1

        if i == 1 or i == len(local_chunks) or i % 25 == 0:
            print(
                f"Drive copy progress: {i}/{len(local_chunks)} "
                f"(uploaded={uploaded}, already={already})"
            )

    local_manifest = write_manifest(
        manifest_path=local_split_dir / f"{filename}.split_{SPLIT_PART_INDEX:04d}.manifest.json",
        original_filename=filename,
        source_url=final_url,
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
    print()
    print("Download this Drive folder now if you want a local copy on your computer.")
    print("After downloading, return to this Colab cell.")

    input("\nPress Enter after you have downloaded or verified this Drive split part... ")

    if DELETE_LOCAL_BATCH_AFTER_CONFIRMATION:
        if yes_no(f"Delete the local Colab filesystem copy at {local_split_dir}?", default=True):
            shutil.rmtree(local_split_dir, ignore_errors=True)
            print("Deleted local Colab copy.")

    if ASK_TO_DELETE_DRIVE_BATCH_AFTER_CONFIRMATION:
        if yes_no(
            f"Delete the Google Drive copy at {drive_split_dir}? "
            "Only do this if you already downloaded it elsewhere.",
            default=False,
        ):
            shutil.rmtree(drive_split_dir, ignore_errors=True)
            print("Deleted Google Drive split folder.")

    print("\nDone.")
    print(f"To continue, set SPLIT_PART_INDEX = {SPLIT_PART_INDEX + 1} and run again.")
    print(f"Total split parts for this file: {total_split_parts}")

main()
