# ============================================================
# Colab URL downloader -> Google Drive chunk batch uploader
# Downloads a direct URL in Drive-sized split batches of chunk files.
# Each split is uploaded, cleaned locally, then optionally removed
# from Drive after you confirm you downloaded it.
# ============================================================

import json
import math
import os
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
from google.colab import drive
from requests.structures import CaseInsensitiveDict


# ============================================================
# USER SETTINGS
# ============================================================

URL = "https://example.com/your-big-file.bin"

# Starting split index. The script can continue automatically after each split.
SPLIT_PART_INDEX = 1

DRIVE_OUTPUT_ROOT = "/content/drive/MyDrive/ColabChunkDownloads"
LOCAL_WORK_ROOT = "/content/download_chunk_work"

CHUNK_SIZE_BYTES = 10_000_000
MAX_LOCAL_BATCH_BYTES = 10_000_000_000
CHUNKS_PER_SPLIT_PART = MAX_LOCAL_BATCH_BYTES // CHUNK_SIZE_BYTES

# When Range is supported and a chunk is large enough, fetch it over several
# connections and assemble it locally before upload.
CHUNK_DOWNLOAD_CONNECTIONS = 4

DELETE_LOCAL_BATCH_AFTER_UPLOAD = True
MAX_RETRIES = 5
REQUEST_STREAM_BYTES = 2 * 1024 * 1024
MIN_PARALLEL_SEGMENT_BYTES = 64 * 1024 * 1024

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

    if len(name) > 120:
        stem = Path(name).stem[:90]
        suffix = Path(name).suffix[:20]
        name = stem + suffix

    return name


def filename_from_headers_or_url(headers, url):
    cd = headers.get("content-disposition", "")

    match = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", cd, flags=re.I)
    if match:
        return safe_filename(unquote(match.group(1)))

    match = re.search(r'filename\s*=\s*"?([^";]+)"?', cd, flags=re.I)
    if match:
        return safe_filename(unquote(match.group(1)))

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
    headers = CaseInsensitiveDict()
    final_url = url
    size = None
    range_supported = False

    try:
        response = session.head(
            url,
            headers=REQUEST_HEADERS,
            allow_redirects=True,
            timeout=30,
        )
        final_url = response.url
        headers.update(response.headers)

        content_length = response.headers.get("Content-Length")
        if content_length and content_length.isdigit():
            size = int(content_length)

    except requests.RequestException as exc:
        print(f"HEAD request failed; continuing with Range check: {exc}")

    range_headers = dict(REQUEST_HEADERS)
    range_headers["Range"] = "bytes=0-0"

    try:
        response = session.get(
            final_url,
            headers=range_headers,
            stream=True,
            allow_redirects=True,
            timeout=30,
        )

        final_url = response.url
        headers.update(response.headers)

        if response.status_code == 206:
            range_supported = True
            content_range = response.headers.get("Content-Range", "")
            match = re.search(r"/(\d+)$", content_range)
            if match:
                size = int(match.group(1))

        response.close()

    except requests.RequestException as exc:
        print(f"Range check failed: {exc}")

    return {
        "final_url": final_url,
        "filename": filename_from_headers_or_url(headers, final_url),
        "size": size,
        "range_supported": range_supported,
    }


def remove_path(path):
    path = Path(path)
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    else:
        path.unlink(missing_ok=True)


def chunk_name(filename, global_chunk_index, total_chunks):
    return f"{filename}.chunk{global_chunk_index:08d}-of-{total_chunks:08d}"


def build_subranges(start, end, connections):
    total = end - start + 1

    if connections <= 1 or total < MIN_PARALLEL_SEGMENT_BYTES:
        return [(0, start, end)]

    max_connections = max(1, min(connections, total // MIN_PARALLEL_SEGMENT_BYTES))
    if max_connections == 1:
        return [(0, start, end)]

    ranges = []
    base = total // max_connections
    extra = total % max_connections
    cursor = start

    for index in range(max_connections):
        size = base + (1 if index < extra else 0)
        seg_start = cursor
        seg_end = cursor + size - 1
        ranges.append((index, seg_start, seg_end))
        cursor = seg_end + 1

    return ranges


def download_subrange(url, start, end, temp_path):
    temp_path = Path(temp_path)
    expected = end - start + 1
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, MAX_RETRIES + 1):
        have = temp_path.stat().st_size if temp_path.exists() else 0

        if have > expected:
            temp_path.unlink()
            have = 0

        if have == expected:
            return temp_path

        headers = dict(REQUEST_HEADERS)
        headers["Range"] = f"bytes={start + have}-{end}"

        try:
            with requests.get(
                url,
                headers=headers,
                stream=True,
                allow_redirects=True,
                timeout=(30, 120),
            ) as response:
                if response.status_code != 206:
                    raise RuntimeError(
                        f"Expected HTTP 206 Partial Content, got HTTP {response.status_code}."
                    )

                with open(temp_path, "ab") as file_obj:
                    for block in response.iter_content(chunk_size=REQUEST_STREAM_BYTES):
                        if block:
                            file_obj.write(block)

            if temp_path.stat().st_size == expected:
                return temp_path

            raise RuntimeError(
                f"Incomplete segment {temp_path.name}: got {temp_path.stat().st_size}, expected {expected}"
            )

        except Exception as exc:
            print(f"Segment attempt {attempt}/{MAX_RETRIES} failed for {temp_path.name}: {exc}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(min(30, 2 ** attempt))

    raise RuntimeError(f"Could not download {temp_path.name}")


def assemble_segments(segment_paths, local_path, expected_size):
    local_path = Path(local_path)
    tmp_path = Path(str(local_path) + ".partial")
    remove_path(tmp_path)

    written = 0
    last_print = 0

    with open(tmp_path, "wb") as output_file:
        for segment_path in segment_paths:
            with open(segment_path, "rb") as input_file:
                while True:
                    block = input_file.read(REQUEST_STREAM_BYTES)
                    if not block:
                        break

                    output_file.write(block)
                    written += len(block)

                    now = time.time()
                    if now - last_print > 1 or written == expected_size:
                        print(
                            f"\rAssemble: {human_size(written)} / {human_size(expected_size)}",
                            end="",
                            flush=True,
                        )
                        last_print = now

    print()
    tmp_path.rename(local_path)


def download_range_single_connection(session, url, start, end, local_path):
    local_path = Path(local_path)
    tmp_path = Path(str(local_path) + ".partial")
    expected = end - start + 1

    if local_path.exists() and local_path.stat().st_size == expected:
        return "already-local"

    if local_path.exists():
        local_path.unlink()

    for attempt in range(1, MAX_RETRIES + 1):
        have = tmp_path.stat().st_size if tmp_path.exists() else 0

        if have > expected:
            tmp_path.unlink()
            have = 0

        if have == expected:
            tmp_path.rename(local_path)
            return "completed-from-partial"

        headers = dict(REQUEST_HEADERS)
        headers["Range"] = f"bytes={start + have}-{end}"

        try:
            with session.get(
                url,
                headers=headers,
                stream=True,
                allow_redirects=True,
                timeout=(30, 120),
            ) as response:
                if response.status_code != 206:
                    raise RuntimeError(
                        f"Expected HTTP 206 Partial Content, got HTTP {response.status_code}."
                    )

                with open(tmp_path, "ab") as file_obj:
                    for block in response.iter_content(chunk_size=REQUEST_STREAM_BYTES):
                        if block:
                            file_obj.write(block)
                            have += len(block)

            if tmp_path.stat().st_size == expected:
                tmp_path.rename(local_path)
                return "downloaded"

        except Exception as exc:
            print(f"Chunk download attempt {attempt}/{MAX_RETRIES} failed: {exc}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(min(30, 2 ** attempt))

    raise RuntimeError(f"Could not download {local_path.name}")


def download_range_to_file(session, url, start, end, local_path, range_supported, connections):
    local_path = Path(local_path)
    expected = end - start + 1

    if local_path.exists() and local_path.stat().st_size == expected:
        return "already-local"

    if local_path.exists():
        local_path.unlink()

    subranges = build_subranges(start, end, connections if range_supported else 1)
    if len(subranges) == 1:
        return download_range_single_connection(session, url, start, end, local_path)

    print(f"Downloading {local_path.name} over {len(subranges)} connections...")

    segment_dir = Path(str(local_path) + ".segments")
    segment_dir.mkdir(parents=True, exist_ok=True)

    try:
        with ThreadPoolExecutor(max_workers=len(subranges)) as executor:
            futures = []
            for index, seg_start, seg_end in subranges:
                segment_path = segment_dir / f"segment_{index:03d}.partial"
                futures.append(executor.submit(download_subrange, url, seg_start, seg_end, segment_path))

            for future in as_completed(futures):
                future.result()

        ordered_paths = [segment_dir / f"segment_{index:03d}.partial" for index, _, _ in subranges]
        assemble_segments(ordered_paths, local_path, expected)

    finally:
        shutil.rmtree(segment_dir, ignore_errors=True)

    if local_path.stat().st_size != expected:
        raise RuntimeError(
            f"Chunk size mismatch for {local_path.name}: got {local_path.stat().st_size}, expected {expected}"
        )

    return "downloaded"


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

    with open(manifest_path, "w", encoding="utf-8") as file_obj:
        json.dump(manifest, file_obj, indent=2)

    return manifest_path


def handle_drive_cleanup(drive_split_dir, current_split, total_split_parts):
    if not yes_no(
        f"Did you download Drive split {current_split}/{total_split_parts}? I want to remove it from Drive.",
        default=False,
    ):
        print("Keeping the current Drive split folder. Stopping here.")
        return False

    remove_path(drive_split_dir)
    print("Removed the current Drive split folder.")

    if current_split >= total_split_parts:
        print("No more split parts remain.")
        return False

    return yes_no("Want to download next part?", default=True)


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

    if CHUNK_DOWNLOAD_CONNECTIONS < 1:
        raise ValueError("CHUNK_DOWNLOAD_CONNECTIONS must be at least 1.")

    print("Mounting Google Drive...")
    drive.mount("/content/drive")

    Path(DRIVE_OUTPUT_ROOT).mkdir(parents=True, exist_ok=True)
    Path(LOCAL_WORK_ROOT).mkdir(parents=True, exist_ok=True)

    session = requests.Session()

    print("\nChecking remote file...")
    info = get_remote_info(session, URL)

    final_url = info["final_url"]
    filename = info["filename"]
    total_size = info["size"]
    range_supported = info["range_supported"]

    print(f"Filename:         {filename}")
    print(f"Final URL:        {final_url}")
    print(f"Remote size:      {human_size(total_size)}")
    print(f"Range support:    {range_supported}")
    print(f"Chunk size:       {human_size(CHUNK_SIZE_BYTES)}")
    print(f"Chunks/split:     {CHUNKS_PER_SPLIT_PART}")
    print(f"Start split:      {SPLIT_PART_INDEX}")
    print(f"Connections:      {CHUNK_DOWNLOAD_CONNECTIONS}")

    if total_size is None:
        raise RuntimeError(
            "The remote file size is unknown. This workflow needs Content-Length or Content-Range."
        )

    if not range_supported:
        raise RuntimeError(
            "This URL does not appear to support HTTP Range requests. "
            "Range support is required for sequential split downloads."
        )

    total_chunks = math.ceil(total_size / CHUNK_SIZE_BYTES)
    total_split_parts = math.ceil(total_chunks / CHUNKS_PER_SPLIT_PART)
    current_split = SPLIT_PART_INDEX
    safe_base = safe_filename(filename)

    while current_split <= total_split_parts:
        start_chunk = (current_split - 1) * CHUNKS_PER_SPLIT_PART + 1
        end_chunk = min(current_split * CHUNKS_PER_SPLIT_PART, total_chunks)

        if start_chunk > total_chunks:
            print(
                f"\nNothing to download. This file has only {total_split_parts} split part(s), "
                f"but SPLIT_PART_INDEX is {current_split}."
            )
            break

        start_byte = (start_chunk - 1) * CHUNK_SIZE_BYTES
        end_byte = min(end_chunk * CHUNK_SIZE_BYTES, total_size) - 1
        batch_size = end_byte - start_byte + 1
        batch_chunk_count = end_chunk - start_chunk + 1

        local_split_dir = Path(LOCAL_WORK_ROOT) / safe_base / f"split_{current_split:04d}"
        drive_split_dir = Path(DRIVE_OUTPUT_ROOT) / safe_base / f"split_{current_split:04d}"
        local_split_dir.mkdir(parents=True, exist_ok=True)
        drive_split_dir.mkdir(parents=True, exist_ok=True)

        print("\nPlanned split part")
        print("------------------")
        print(f"Split part:       {current_split} of {total_split_parts}")
        print(f"Chunks:           {start_chunk} through {end_chunk}")
        print(f"Chunk count:      {batch_chunk_count}")
        print(f"Byte range:       {start_byte} through {end_byte}")
        print(f"Batch size:       {human_size(batch_size)}")
        print(f"Local folder:     {local_split_dir}")
        print(f"Drive folder:     {drive_split_dir}")

        print("\nDownloading chunks into the Colab filesystem...")

        for global_chunk_index in range(start_chunk, end_chunk + 1):
            chunk_start = (global_chunk_index - 1) * CHUNK_SIZE_BYTES
            chunk_end = min(global_chunk_index * CHUNK_SIZE_BYTES, total_size) - 1
            expected = chunk_end - chunk_start + 1

            name = chunk_name(filename, global_chunk_index, total_chunks)
            local_chunk = local_split_dir / name
            drive_chunk = drive_split_dir / name

            if drive_chunk.exists() and drive_chunk.stat().st_size == expected:
                if global_chunk_index == start_chunk or global_chunk_index % 25 == 0:
                    print(f"Drive already has chunk {global_chunk_index}/{total_chunks}; skipping local download.")
                continue

            status = download_range_to_file(
                session=session,
                url=final_url,
                start=chunk_start,
                end=chunk_end,
                local_path=local_chunk,
                range_supported=range_supported,
                connections=CHUNK_DOWNLOAD_CONNECTIONS,
            )

            if (
                global_chunk_index == start_chunk
                or global_chunk_index == end_chunk
                or global_chunk_index % 25 == 0
            ):
                done_in_batch = global_chunk_index - start_chunk + 1
                print(
                    f"Chunk {global_chunk_index}/{total_chunks} ({done_in_batch}/{batch_chunk_count}) {status}: {local_chunk.name}"
                )

        local_chunks = sorted(local_split_dir.glob(f"{filename}.chunk*-of-{total_chunks:08d}"))

        print("\nCopying chunks to Google Drive...")
        uploaded = 0
        already = 0

        for index, local_chunk in enumerate(local_chunks, start=1):
            drive_chunk = drive_split_dir / local_chunk.name
            status = copy_file_to_drive(local_chunk, drive_chunk)

            if status == "uploaded":
                uploaded += 1
            else:
                already += 1

            if index == 1 or index == len(local_chunks) or index % 25 == 0:
                print(
                    f"Drive copy progress: {index}/{len(local_chunks)} (uploaded={uploaded}, already={already})"
                )

        local_manifest = write_manifest(
            manifest_path=local_split_dir / f"{filename}.split_{current_split:04d}.manifest.json",
            original_filename=filename,
            source_url=final_url,
            total_size=total_size,
            chunk_size=CHUNK_SIZE_BYTES,
            split_part_index=current_split,
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

        if DELETE_LOCAL_BATCH_AFTER_UPLOAD:
            remove_path(local_split_dir)
            print("Removed local temporary files for this split.")

        should_continue = handle_drive_cleanup(
            drive_split_dir=drive_split_dir,
            current_split=current_split,
            total_split_parts=total_split_parts,
        )

        if not should_continue:
            break

        current_split += 1


main()
