# ============================================================
# Colab URL downloader -> Google Drive chunk batch uploader
# Downloads a direct URL in Drive-sized split batches of chunk files.
#
# Split download model:
#   - One split covers a contiguous byte range of the remote file.
#   - The split range is divided across several workers.
#   - Each worker issues exactly one HTTP Range request.
#   - Each worker writes its streamed response directly into the final chunk files.
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

# Number of concurrent split-range requests.
SPLIT_DOWNLOAD_CONNECTIONS = 4

# If False, an existing local chunk with the expected size is reused and a
# matching Drive chunk is skipped. If True, the current split is redownloaded
# and reuploaded from scratch.
ENABLE_REDOWNLOAD = False

DELETE_LOCAL_BATCH_AFTER_UPLOAD = True
MAX_RETRIES = 5
REQUEST_STREAM_BYTES = 2 * 1024 * 1024
MIN_REQUEST_BYTES = 64 * 1024 * 1024

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
    content_disposition = headers.get("content-disposition", "")

    match = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", content_disposition, flags=re.I)
    if match:
        return safe_filename(unquote(match.group(1)))

    match = re.search(r'filename\s*=\s*"?([^";]+)"?', content_disposition, flags=re.I)
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


def size_matches(path, expected_size):
    path = Path(path)
    return path.exists() and path.is_file() and path.stat().st_size == expected_size


def copy_file_to_drive(local_path, drive_path, force_upload=False):
    local_path = Path(local_path)
    drive_path = Path(drive_path)
    drive_path.parent.mkdir(parents=True, exist_ok=True)

    expected_size = local_path.stat().st_size

    if not force_upload and size_matches(drive_path, expected_size):
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
            "Matching local chunk files are reused unless ENABLE_REDOWNLOAD is set to True.",
        ],
    }

    manifest_path = Path(manifest_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    with open(manifest_path, "w", encoding="utf-8") as file_obj:
        json.dump(manifest, file_obj, indent=2)

    return manifest_path


def build_split_chunk_infos(
    filename,
    total_chunks,
    total_size,
    start_chunk,
    end_chunk,
    local_split_dir,
    drive_split_dir,
):
    chunk_infos = []

    for global_chunk_index in range(start_chunk, end_chunk + 1):
        chunk_start = (global_chunk_index - 1) * CHUNK_SIZE_BYTES
        chunk_end = min(global_chunk_index * CHUNK_SIZE_BYTES, total_size) - 1
        expected_size = chunk_end - chunk_start + 1
        name = chunk_name(filename, global_chunk_index, total_chunks)

        chunk_infos.append(
            {
                "chunk_index": global_chunk_index,
                "name": name,
                "start_byte": chunk_start,
                "end_byte": chunk_end,
                "expected_size": expected_size,
                "local_path": Path(local_split_dir) / name,
                "drive_path": Path(drive_split_dir) / name,
            }
        )

    return chunk_infos


def prepare_chunk_states(chunk_infos):
    states = []

    for chunk in chunk_infos:
        state = dict(chunk)
        local_path = state["local_path"]
        drive_path = state["drive_path"]
        expected_size = state["expected_size"]

        if ENABLE_REDOWNLOAD:
            remove_path(local_path)
            state["drive_ready"] = False
            state["local_size"] = 0
            state["local_ready"] = False
            state["needs_download"] = True
            states.append(state)
            continue

        state["drive_ready"] = size_matches(drive_path, expected_size)

        local_size = 0
        if local_path.exists():
            local_size = local_path.stat().st_size

            if local_size > expected_size:
                print(f"Deleting oversized local chunk: {local_path.name}")
                local_path.unlink()
                local_size = 0

        state["local_size"] = local_size
        state["local_ready"] = local_size == expected_size
        state["needs_download"] = (not state["drive_ready"]) and (local_size < expected_size)
        states.append(state)

    return states


def build_download_runs(states):
    runs = []
    current_run = []
    previous_chunk_index = None

    for state in states:
        if not state["needs_download"]:
            if current_run:
                runs.append(current_run)
                current_run = []
            previous_chunk_index = None
            continue

        starts_with_partial = state["local_size"] > 0

        if current_run:
            contiguous = state["chunk_index"] == previous_chunk_index + 1
            if (not contiguous) or starts_with_partial:
                runs.append(current_run)
                current_run = []

        current_run.append(state)
        previous_chunk_index = state["chunk_index"]

    if current_run:
        runs.append(current_run)

    return runs


def split_contiguous_run(run, max_request_count):
    if not run:
        return []

    remaining_bytes = run[-1]["end_byte"] - (run[0]["start_byte"] + run[0]["local_size"]) + 1
    if remaining_bytes <= 0:
        return []

    desired_request_count = 1
    if len(run) > 1 and remaining_bytes >= MIN_REQUEST_BYTES:
        desired_request_count = min(max_request_count, len(run))

    groups = []
    base = len(run) // desired_request_count
    extra = len(run) % desired_request_count
    cursor = 0

    for index in range(desired_request_count):
        group_size = base + (1 if index < extra else 0)
        group = run[cursor: cursor + group_size]
        if group:
            groups.append(group)
        cursor += group_size

    return groups


def build_download_tasks(states):
    tasks = []
    task_counter = 1
    runs = build_download_runs(states)

    for run in runs:
        groups = split_contiguous_run(run, SPLIT_DOWNLOAD_CONNECTIONS)
        for group in groups:
            first_chunk = group[0]
            start_byte = first_chunk["start_byte"] + first_chunk["local_size"]
            end_byte = group[-1]["end_byte"]

            tasks.append(
                {
                    "task_number": task_counter,
                    "chunk_start_index": group[0]["chunk_index"],
                    "chunk_end_index": group[-1]["chunk_index"],
                    "start_byte": start_byte,
                    "end_byte": end_byte,
                    "chunks": group,
                }
            )
            task_counter += 1

    return tasks


def download_task_to_chunks(url, task):
    headers = dict(REQUEST_HEADERS)
    headers["Range"] = f"bytes={task['start_byte']}-{task['end_byte']}"
    expected_total = task["end_byte"] - task["start_byte"] + 1

    print(
        f"Request {task['task_number']}: chunks {task['chunk_start_index']}-{task['chunk_end_index']} "
        f"bytes {task['start_byte']}-{task['end_byte']} ({human_size(expected_total)})"
    )

    for attempt in range(1, MAX_RETRIES + 1):
        bytes_written = 0
        current_file = None

        try:
            chunks = task["chunks"]
            chunk_position = 0
            current_chunk = chunks[chunk_position]
            current_written = current_chunk["local_size"]
            current_target = current_chunk["expected_size"]

            current_chunk["local_path"].parent.mkdir(parents=True, exist_ok=True)
            current_file = open(
                current_chunk["local_path"],
                "ab" if current_written > 0 else "wb",
            )

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

                for block in response.iter_content(chunk_size=REQUEST_STREAM_BYTES):
                    if not block:
                        continue

                    view = memoryview(block)

                    while view:
                        remaining_in_chunk = current_target - current_written
                        piece = view[:remaining_in_chunk]
                        current_file.write(piece)
                        current_written += len(piece)
                        bytes_written += len(piece)
                        view = view[len(piece):]

                        if current_written == current_target:
                            current_file.close()
                            current_file = None
                            chunk_position += 1

                            if chunk_position == len(chunks):
                                if view:
                                    raise RuntimeError("Received more bytes than expected for the task.")
                                break

                            current_chunk = chunks[chunk_position]
                            current_written = current_chunk["local_size"]
                            current_target = current_chunk["expected_size"]
                            if current_written != 0:
                                raise RuntimeError(
                                    f"Only the first chunk in a request may resume, but {current_chunk['name']} has partial data."
                                )

                            current_chunk["local_path"].parent.mkdir(parents=True, exist_ok=True)
                            current_file = open(current_chunk["local_path"], "wb")

            if current_file is not None:
                current_file.close()
                current_file = None

            if bytes_written != expected_total:
                raise RuntimeError(
                    f"Incomplete request {task['task_number']}: got {bytes_written}, expected {expected_total}"
                )

            for chunk in task["chunks"]:
                if not size_matches(chunk["local_path"], chunk["expected_size"]):
                    raise RuntimeError(
                        f"Chunk size mismatch for {chunk['name']}: "
                        f"got {chunk['local_path'].stat().st_size if chunk['local_path'].exists() else 'missing'}, "
                        f"expected {chunk['expected_size']}"
                    )

            print(f"Request {task['task_number']} completed.")
            return

        except Exception as exc:
            if current_file is not None:
                current_file.close()

            print(f"Request {task['task_number']} attempt {attempt}/{MAX_RETRIES} failed: {exc}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(min(30, 2 ** attempt))


def download_split_to_local_chunks(final_url, states):
    drive_ready_count = sum(1 for state in states if state["drive_ready"])
    local_ready_count = sum(
        1 for state in states
        if (not state["drive_ready"]) and state["local_ready"]
    )
    partial_count = sum(
        1 for state in states
        if (not state["drive_ready"]) and 0 < state["local_size"] < state["expected_size"]
    )
    missing_count = sum(1 for state in states if state["needs_download"] and state["local_size"] == 0)

    print(f"Drive-ready chunks: {drive_ready_count}")
    print(f"Local-ready chunks: {local_ready_count}")
    print(f"Partial local chunks: {partial_count}")
    print(f"Missing chunks: {missing_count}")

    tasks = build_download_tasks(states)
    if not tasks:
        print("No chunk downloads are needed for this split.")
        return

    print(f"Launching {len(tasks)} split-range request(s) with up to {SPLIT_DOWNLOAD_CONNECTIONS} concurrent worker(s)...")

    with ThreadPoolExecutor(max_workers=min(SPLIT_DOWNLOAD_CONNECTIONS, len(tasks))) as executor:
        futures = [executor.submit(download_task_to_chunks, final_url, task) for task in tasks]
        for future in as_completed(futures):
            future.result()

    for state in states:
        if state["drive_ready"]:
            continue

        if not size_matches(state["local_path"], state["expected_size"]):
            raise RuntimeError(
                f"Local chunk is incomplete after split download: {state['name']}"
            )


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

    if SPLIT_DOWNLOAD_CONNECTIONS < 1:
        raise ValueError("SPLIT_DOWNLOAD_CONNECTIONS must be at least 1.")

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

    print(f"Filename:              {filename}")
    print(f"Final URL:             {final_url}")
    print(f"Remote size:           {human_size(total_size)}")
    print(f"Range support:         {range_supported}")
    print(f"Chunk size:            {human_size(CHUNK_SIZE_BYTES)}")
    print(f"Chunks/split:          {CHUNKS_PER_SPLIT_PART}")
    print(f"Start split:           {SPLIT_PART_INDEX}")
    print(f"Split connections:     {SPLIT_DOWNLOAD_CONNECTIONS}")
    print(f"Enable redownload:     {ENABLE_REDOWNLOAD}")

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
        print(f"Split part:           {current_split} of {total_split_parts}")
        print(f"Chunks:               {start_chunk} through {end_chunk}")
        print(f"Chunk count:          {batch_chunk_count}")
        print(f"Byte range:           {start_byte} through {end_byte}")
        print(f"Batch size:           {human_size(batch_size)}")
        print(f"Local folder:         {local_split_dir}")
        print(f"Drive folder:         {drive_split_dir}")

        split_chunks = build_split_chunk_infos(
            filename=filename,
            total_chunks=total_chunks,
            total_size=total_size,
            start_chunk=start_chunk,
            end_chunk=end_chunk,
            local_split_dir=local_split_dir,
            drive_split_dir=drive_split_dir,
        )
        chunk_states = prepare_chunk_states(split_chunks)

        print("\nDownloading the split range into chunk files...")
        download_split_to_local_chunks(final_url, chunk_states)

        print("\nCopying chunks to Google Drive...")
        uploaded = 0
        already = 0
        skipped_drive = 0

        chunks_to_upload = [state for state in chunk_states if ENABLE_REDOWNLOAD or not state["drive_ready"]]

        for index, state in enumerate(chunks_to_upload, start=1):
            status = copy_file_to_drive(
                local_path=state["local_path"],
                drive_path=state["drive_path"],
                force_upload=ENABLE_REDOWNLOAD,
            )

            if status == "uploaded":
                uploaded += 1
            else:
                already += 1

            if index == 1 or index == len(chunks_to_upload) or index % 25 == 0:
                print(
                    f"Drive copy progress: {index}/{len(chunks_to_upload)} "
                    f"(uploaded={uploaded}, already={already})"
                )

        skipped_drive = sum(1 for state in chunk_states if state["drive_ready"] and not ENABLE_REDOWNLOAD)

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
        copy_file_to_drive(local_manifest, drive_manifest, force_upload=ENABLE_REDOWNLOAD)

        print("\nThis split part is now in Google Drive.")
        print(f"Drive folder:          {drive_split_dir}")
        print(f"Manifest:              {drive_manifest.name}")
        print(f"Uploaded chunks:       {uploaded}")
        print(f"Already on Drive:      {already}")
        print(f"Skipped by Drive match:{skipped_drive}")

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
