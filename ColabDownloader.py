# ============================================================
# Colab URL downloader -> Google Drive uploader
# Downloads one file from a direct URL and uploads it to Drive.
# Large files are processed one part at a time in sequence.
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


# -------------------------
# EDIT THESE SETTINGS
# -------------------------

URL = "https://example.com/your-big-file.zip"

DRIVE_OUTPUT_DIR = "/content/drive/MyDrive/ColabDownloads"
LOCAL_WORK_DIR = "/content/download_work"

# 10 GiB threshold. For decimal 10 GB, use: 10_000_000_000
MAX_SINGLE_FILE_BYTES = 10 * 1024**3

# Use slightly under 10 GiB for parts to leave room for filesystem overhead.
PART_SIZE_BYTES = int(9.5 * 1024**3)

# For large files, start from this part number and continue automatically.
START_PART_INDEX = 1

# When HTTP Range is supported, each file or part can be fetched over
# several connections and then assembled locally.
PART_DOWNLOAD_CONNECTIONS = 4

DELETE_LOCAL_AFTER_UPLOAD = True
MAX_RETRIES = 5
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
MIN_PARALLEL_SEGMENT_BYTES = 64 * 1024 * 1024

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 ColabDownloader/1.0"
}


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
    return name or "downloaded_file"


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
        print(f"HEAD request failed, continuing with Range check: {exc}")

    try:
        range_headers = dict(REQUEST_HEADERS)
        range_headers["Range"] = "bytes=0-0"

        response = session.get(
            url,
            headers=range_headers,
            stream=True,
            allow_redirects=True,
            timeout=30,
        )

        final_url = response.url

        if response.status_code == 206:
            range_supported = True
            content_range = response.headers.get("Content-Range", "")
            match = re.search(r"/(\d+)$", content_range)
            if match:
                size = int(match.group(1))

            if not headers.get("content-disposition") and response.headers.get("content-disposition"):
                headers["content-disposition"] = response.headers["content-disposition"]

        response.close()

    except requests.RequestException as exc:
        print(f"Range support check failed: {exc}")

    return {
        "final_url": final_url,
        "size": size,
        "range_supported": range_supported,
        "filename": filename_from_headers_or_url(headers, final_url),
    }


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
                    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                        if chunk:
                            file_obj.write(chunk)

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

    raise RuntimeError(f"Failed to download segment {temp_path.name}")


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
                    block = input_file.read(DOWNLOAD_CHUNK_BYTES)
                    if not block:
                        break

                    output_file.write(block)
                    written += len(block)

                    now = time.time()
                    if now - last_print > 1 or written == expected_size:
                        print_progress("Assemble", written, expected_size)
                        last_print = now

    print()
    tmp_path.rename(local_path)


def download_range_single_connection(session, url, start, end, local_path):
    local_path = Path(local_path)
    tmp_path = Path(str(local_path) + ".partial")
    expected = end - start + 1

    if local_path.exists() and local_path.stat().st_size == expected:
        return local_path

    if local_path.exists():
        local_path.unlink()

    for attempt in range(1, MAX_RETRIES + 1):
        have = tmp_path.stat().st_size if tmp_path.exists() else 0

        if have > expected:
            tmp_path.unlink()
            have = 0

        if have == expected:
            tmp_path.rename(local_path)
            return local_path

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
                        f"Expected HTTP 206 Partial Content, got {response.status_code}."
                    )

                last_print = 0
                with open(tmp_path, "ab") as file_obj:
                    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                        if not chunk:
                            continue

                        file_obj.write(chunk)
                        have += len(chunk)

                        now = time.time()
                        if now - last_print > 1 or have == expected:
                            print_progress("Download", have, expected)
                            last_print = now

            print()

            if tmp_path.stat().st_size == expected:
                tmp_path.rename(local_path)
                return local_path

        except Exception as exc:
            print(f"Attempt {attempt}/{MAX_RETRIES} failed: {exc}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(min(30, 2 ** attempt))

    raise RuntimeError(f"Failed to download range for {local_path.name}")


def download_range_to_file(session, url, start, end, local_path, range_supported, connections):
    local_path = Path(local_path)
    expected = end - start + 1

    if local_path.exists() and local_path.stat().st_size == expected:
        print(f"\nLocal file already complete: {local_path.name}")
        return local_path

    if local_path.exists():
        local_path.unlink()

    subranges = build_subranges(start, end, connections if range_supported else 1)
    if len(subranges) == 1:
        print(f"\nDownloading {local_path.name} over one connection...")
        return download_range_single_connection(session, url, start, end, local_path)

    print(f"\nDownloading {local_path.name} over {len(subranges)} connections...")

    segment_dir = Path(str(local_path) + ".segments")
    segment_dir.mkdir(parents=True, exist_ok=True)
    segment_paths = []

    try:
        with ThreadPoolExecutor(max_workers=len(subranges)) as executor:
            futures = []

            for index, seg_start, seg_end in subranges:
                segment_path = segment_dir / f"segment_{index:03d}.partial"
                segment_paths.append(segment_path)
                futures.append(executor.submit(download_subrange, url, seg_start, seg_end, segment_path))

            for future in as_completed(futures):
                future.result()

        ordered_paths = [segment_dir / f"segment_{index:03d}.partial" for index, _, _ in subranges]
        assemble_segments(ordered_paths, local_path, expected)

    finally:
        shutil.rmtree(segment_dir, ignore_errors=True)

    actual_size = local_path.stat().st_size
    if actual_size != expected:
        raise RuntimeError(f"Downloaded size mismatch: got {actual_size}, expected {expected}")

    return local_path


def download_whole_to_file(session, url, local_path, expected_size=None, range_supported=False):
    local_path = Path(local_path)

    if expected_size is not None and range_supported:
        return download_range_to_file(
            session=session,
            url=url,
            start=0,
            end=expected_size - 1,
            local_path=local_path,
            range_supported=True,
            connections=PART_DOWNLOAD_CONNECTIONS,
        )

    if local_path.exists() and expected_size is not None and local_path.stat().st_size == expected_size:
        return local_path

    tmp_path = Path(str(local_path) + ".partial")
    remove_path(local_path)
    remove_path(tmp_path)

    print(f"\nDownloading {local_path.name} over one connection...")

    with session.get(
        url,
        headers=REQUEST_HEADERS,
        stream=True,
        allow_redirects=True,
        timeout=(30, 120),
    ) as response:
        response.raise_for_status()

        total = expected_size
        if total is None:
            content_length = response.headers.get("Content-Length")
            total = int(content_length) if content_length and content_length.isdigit() else None

        done = 0
        last_print = 0

        with open(tmp_path, "wb") as file_obj:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if not chunk:
                    continue

                file_obj.write(chunk)
                done += len(chunk)

                now = time.time()
                if now - last_print > 1 or done == total:
                    print_progress("Download", done, total)
                    last_print = now

    print()
    tmp_path.rename(local_path)

    if expected_size is not None and local_path.stat().st_size != expected_size:
        raise RuntimeError(
            f"Downloaded size mismatch: got {local_path.stat().st_size}, expected {expected_size}"
        )

    return local_path


def write_manifest(local_dir, filename, source_url, size, part_size, total_parts):
    manifest = {
        "original_filename": filename,
        "source_url": source_url,
        "original_size_bytes": size,
        "original_size_human": human_size(size),
        "part_size_bytes": part_size,
        "part_size_human": human_size(part_size),
        "total_parts": total_parts,
        "part_name_pattern": f"{filename}.part0001-of-{total_parts:04d}",
        "rebuild_order": [
            f"{filename}.part{i:04d}-of-{total_parts:04d}"
            for i in range(1, total_parts + 1)
        ],
    }

    manifest_path = Path(local_dir) / f"{filename}.manifest.json"
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


def process_large_file_in_parts(session, url, filename, size, range_supported):
    if size is None:
        raise RuntimeError("Large-file part mode needs a known remote size.")

    if not range_supported:
        raise RuntimeError(
            "This server does not appear to support HTTP Range requests. "
            "Part-by-part download needs Range support."
        )

    total_parts = math.ceil(size / PART_SIZE_BYTES)
    current_part = max(1, START_PART_INDEX)

    print("\nLarge file detected.")
    print(f"Original file: {filename}")
    print(f"Remote size:   {human_size(size)}")
    print(f"Part size:     {human_size(PART_SIZE_BYTES)}")
    print(f"Total parts:   {total_parts}")
    print(f"Start part:    {current_part}")

    while current_part <= total_parts:
        start = (current_part - 1) * PART_SIZE_BYTES
        end = min(start + PART_SIZE_BYTES - 1, size - 1)
        expected = end - start + 1

        part_name = f"{filename}.part{current_part:04d}-of-{total_parts:04d}"
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
            download_range_to_file(
                session=session,
                url=url,
                start=start,
                end=end,
                local_path=local_part,
                range_supported=range_supported,
                connections=PART_DOWNLOAD_CONNECTIONS,
            )
            copy_file_to_drive(local_part, drive_part)

        local_manifest = write_manifest(
            local_dir=LOCAL_WORK_DIR,
            filename=filename,
            source_url=url,
            size=size,
            part_size=PART_SIZE_BYTES,
            total_parts=total_parts,
        )
        drive_manifest = Path(DRIVE_OUTPUT_DIR) / local_manifest.name
        copy_file_to_drive(local_manifest, drive_manifest)

        if DELETE_LOCAL_AFTER_UPLOAD:
            remove_path(local_part)
            remove_path(Path(str(local_part) + ".partial"))
            remove_path(local_manifest)
            print("Removed local temporary files for this part.")

        should_continue = handle_drive_cleanup(
            drive_paths=[drive_part, drive_manifest],
            label=f"Drive part {current_part}/{total_parts} and its manifest",
            has_next_part=current_part < total_parts,
        )

        if not should_continue:
            break

        current_part += 1


def process_single_file(session, url, filename, size, range_supported):
    local_file = Path(LOCAL_WORK_DIR) / filename
    drive_file = Path(DRIVE_OUTPUT_DIR) / filename

    download_whole_to_file(
        session=session,
        url=url,
        local_path=local_file,
        expected_size=size,
        range_supported=range_supported,
    )
    copy_file_to_drive(local_file, drive_file)

    if DELETE_LOCAL_AFTER_UPLOAD:
        remove_path(local_file)
        remove_path(Path(str(local_file) + ".partial"))
        print("Removed local temporary files.")

    handle_drive_cleanup(
        drive_paths=[drive_file],
        label=f"the Drive file {filename}",
        has_next_part=False,
    )


def main():
    if MAX_SINGLE_FILE_BYTES <= 0:
        raise ValueError("MAX_SINGLE_FILE_BYTES must be greater than zero.")

    if PART_SIZE_BYTES <= 0:
        raise ValueError("PART_SIZE_BYTES must be greater than zero.")

    if START_PART_INDEX < 1:
        raise ValueError("START_PART_INDEX must be 1 or greater.")

    if PART_DOWNLOAD_CONNECTIONS < 1:
        raise ValueError("PART_DOWNLOAD_CONNECTIONS must be 1 or greater.")

    print("Mounting Google Drive...")
    drive.mount("/content/drive")

    Path(DRIVE_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    Path(LOCAL_WORK_DIR).mkdir(parents=True, exist_ok=True)

    session = requests.Session()

    print("\nChecking remote file...")
    info = get_remote_info(session, URL)

    final_url = info["final_url"]
    size = info["size"]
    range_supported = info["range_supported"]
    filename = info["filename"]

    print(f"Filename:        {filename}")
    print(f"Final URL:       {final_url}")
    print(f"Remote size:     {human_size(size)}")
    print(f"Range support:   {range_supported}")
    print(f"Connections:     {PART_DOWNLOAD_CONNECTIONS}")
    print(f"Drive folder:    {DRIVE_OUTPUT_DIR}")
    print(f"Local work dir:  {LOCAL_WORK_DIR}")

    if size is not None and size > MAX_SINGLE_FILE_BYTES:
        process_large_file_in_parts(
            session=session,
            url=final_url,
            filename=filename,
            size=size,
            range_supported=range_supported,
        )
    else:
        process_single_file(
            session=session,
            url=final_url,
            filename=filename,
            size=size,
            range_supported=range_supported,
        )


main()
