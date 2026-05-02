# ============================================================
# Colab URL downloader -> Google Drive uploader
# Splits files larger than 10 GB into uploadable parts.
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

# Delete each local part after it has been copied to Google Drive.
DELETE_LOCAL_AFTER_UPLOAD = True

# Retry network operations.
MAX_RETRIES = 5

# 8 MiB download buffer.
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024

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

    # RFC 5987 style: filename*=UTF-8''file.zip
    m = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", cd, flags=re.I)
    if m:
        return safe_filename(unquote(m.group(1)))

    # Basic style: filename="file.zip"
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


def parse_part_selection(text, total_parts):
    """
    Accepts:
      blank      -> all parts
      3          -> part 3
      1,4,7      -> parts 1, 4, 7
      2-5        -> parts 2, 3, 4, 5
      1,3-5,9    -> mixed
    """
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
            f"\r{label}: {human_size(done)} / {human_size(total)} "
            f"({pct:.1f}%)",
            end="",
            flush=True,
        )
    else:
        print(f"\r{label}: {human_size(done)}", end="", flush=True)


def get_remote_info(session, url):
    """
    Returns:
      final_url
      size
      range_supported
      filename
      headers
    """
    headers = CaseInsensitiveDict()
    final_url = url
    size = None
    range_supported = False

    # Try HEAD first.
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
        print(f"HEAD request failed, continuing with Range check: {exc}")

    # Check HTTP Range support with a 1-byte request.
    try:
        range_headers = dict(REQUEST_HEADERS)
        range_headers["Range"] = "bytes=0-0"

        r = session.get(
            url,
            headers=range_headers,
            stream=True,
            allow_redirects=True,
            timeout=30,
        )

        final_url = r.url

        if r.status_code == 206:
            range_supported = True

            # Content-Range usually looks like: bytes 0-0/123456789
            cr = r.headers.get("Content-Range", "")
            m = re.search(r"/(\d+)$", cr)
            if m:
                size = int(m.group(1))

            if not headers.get("content-disposition") and r.headers.get("content-disposition"):
                headers["content-disposition"] = r.headers["content-disposition"]

        r.close()

    except requests.RequestException as exc:
        print(f"Range support check failed: {exc}")

    filename = filename_from_headers_or_url(headers, final_url)

    return {
        "final_url": final_url,
        "size": size,
        "range_supported": range_supported,
        "filename": filename,
        "headers": headers,
    }


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

    # Force filesystem sync where possible.
    os.sync()

    local_size = local_path.stat().st_size
    drive_size = drive_path.stat().st_size

    if drive_size != local_size:
        raise RuntimeError(
            f"Upload verification failed for {drive_path.name}: "
            f"local={local_size}, drive={drive_size}"
        )

    print(f"Uploaded: {drive_path} ({human_size(drive_size)})")
    return drive_path


def download_whole_to_file(session, url, local_path, expected_size=None):
    local_path = Path(local_path)
    tmp_path = Path(str(local_path) + ".partial")

    if local_path.exists():
        if yes_no(f"Local file already exists: {local_path.name}. Reuse it?", default=True):
            return local_path
        local_path.unlink()

    if tmp_path.exists():
        if yes_no(f"Partial file exists: {tmp_path.name}. Delete and restart?", default=True):
            tmp_path.unlink()
        else:
            raise RuntimeError("Stopped because partial file exists and restart was declined.")

    print(f"\nDownloading whole file to local disk: {local_path}")

    with session.get(
        url,
        headers=REQUEST_HEADERS,
        stream=True,
        allow_redirects=True,
        timeout=(30, 120),
    ) as r:
        r.raise_for_status()

        total = expected_size
        if total is None:
            cl = r.headers.get("Content-Length")
            total = int(cl) if cl and cl.isdigit() else None

        done = 0
        last_print = 0

        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if not chunk:
                    continue

                f.write(chunk)
                done += len(chunk)

                now = time.time()
                if now - last_print > 1 or done == total:
                    print_progress("Download", done, total)
                    last_print = now

    print()
    tmp_path.rename(local_path)

    if expected_size is not None and local_path.stat().st_size != expected_size:
        raise RuntimeError(
            f"Downloaded size mismatch: got {local_path.stat().st_size}, "
            f"expected {expected_size}"
        )

    return local_path


def download_range_to_file(session, url, start, end, local_path):
    """
    Downloads byte range [start, end] inclusive.
    Requires server to support HTTP Range.
    Resumes from .partial file when possible.
    """
    local_path = Path(local_path)
    tmp_path = Path(str(local_path) + ".partial")
    expected = end - start + 1

    if local_path.exists() and local_path.stat().st_size == expected:
        print(f"\nLocal part already complete: {local_path.name}")
        return local_path

    if local_path.exists():
        print(f"\nLocal part exists but size is wrong; deleting: {local_path.name}")
        local_path.unlink()

    for attempt in range(1, MAX_RETRIES + 1):
        have = tmp_path.stat().st_size if tmp_path.exists() else 0

        if have > expected:
            print("Partial file is larger than expected; deleting and restarting this part.")
            tmp_path.unlink()
            have = 0

        if have == expected:
            tmp_path.rename(local_path)
            return local_path

        range_start = start + have
        headers = dict(REQUEST_HEADERS)
        headers["Range"] = f"bytes={range_start}-{end}"

        print(
            f"\nDownloading {local_path.name} "
            f"bytes {range_start}-{end} "
            f"({human_size(expected)})"
        )

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
                        f"Expected HTTP 206 Partial Content, got {r.status_code}. "
                        "This server may not support byte-range downloads."
                    )

                last_print = 0

                with open(tmp_path, "ab") as f:
                    for chunk in r.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                        if not chunk:
                            continue

                        f.write(chunk)
                        have += len(chunk)

                        now = time.time()
                        if now - last_print > 1 or have == expected:
                            print_progress("Part download", have, expected)
                            last_print = now

            print()

            if tmp_path.stat().st_size == expected:
                tmp_path.rename(local_path)
                return local_path

            print(
                f"Part incomplete after attempt {attempt}: "
                f"{human_size(tmp_path.stat().st_size)} / {human_size(expected)}"
            )

        except Exception as exc:
            print(f"\nAttempt {attempt}/{MAX_RETRIES} failed: {exc}")

            if attempt == MAX_RETRIES:
                raise

            sleep_seconds = min(30, 2 ** attempt)
            print(f"Retrying in {sleep_seconds} seconds...")
            time.sleep(sleep_seconds)

    raise RuntimeError(f"Failed to download range for {local_path.name}")


def write_and_upload_manifest(filename, url, size, part_size, total_parts, drive_output_dir):
    manifest = {
        "original_filename": filename,
        "source_url": url,
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

    manifest_path = Path(LOCAL_WORK_DIR) / f"{filename}.manifest.json"

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    upload_to_drive(manifest_path, drive_output_dir)

    if DELETE_LOCAL_AFTER_UPLOAD:
        manifest_path.unlink(missing_ok=True)


def process_large_file_in_parts(session, url, filename, size, range_supported):
    if size is None:
        raise RuntimeError(
            "The remote file size is unknown. Large-file part mode needs a known size. "
            "Try a direct-download URL that returns Content-Length or Content-Range."
        )

    if not range_supported:
        raise RuntimeError(
            "The server does not appear to support HTTP Range requests. "
            "Part-by-part download needs Range support so Colab can fetch one part, "
            "upload it, delete it, and then fetch the next part without restarting "
            "from byte 0."
        )

    total_parts = math.ceil(size / PART_SIZE_BYTES)

    print("\nLarge file detected.")
    print(f"Original file: {filename}")
    print(f"Remote size:   {human_size(size)}")
    print(f"Part size:     {human_size(PART_SIZE_BYTES)}")
    print(f"Total parts:   {total_parts}")
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

        part_name = f"{filename}.part{part_number:04d}-of-{total_parts:04d}"
        local_part = Path(LOCAL_WORK_DIR) / part_name
        drive_part = Path(DRIVE_OUTPUT_DIR) / part_name

        print("\n" + "=" * 70)
        print(f"Part {part_number}/{total_parts}")
        print(f"Name:      {part_name}")
        print(f"Byte range:{start}-{end}")
        print(f"Size:      {human_size(expected)}")

        if drive_part.exists() and drive_part.stat().st_size == expected:
            if yes_no(f"Part already exists in Drive with the right size. Skip part {part_number}?", default=True):
                continue

        if not yes_no(f"Download part {part_number} now?", default=True):
            print(f"Skipped part {part_number}.")
            continue

        download_range_to_file(session, url, start, end, local_part)

        if yes_no(f"Upload {part_name} to Google Drive now?", default=True):
            upload_to_drive(local_part, DRIVE_OUTPUT_DIR)

            if DELETE_LOCAL_AFTER_UPLOAD:
                local_part.unlink(missing_ok=True)
                partial_path = Path(str(local_part) + ".partial")
                partial_path.unlink(missing_ok=True)
                print(f"Deleted local part to free space: {local_part}")

        else:
            print(f"Kept local part here: {local_part}")
            print("You can manually download, move, or delete it before continuing.")

            input("Press Enter after you have freed space, or stop the cell to quit.")

    print("\nWriting manifest file to Google Drive...")
    write_and_upload_manifest(
        filename=filename,
        url=url,
        size=size,
        part_size=PART_SIZE_BYTES,
        total_parts=total_parts,
        drive_output_dir=DRIVE_OUTPUT_DIR,
    )

    print("\nDone with selected large-file parts.")


def main():
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

    print(f"Filename:       {filename}")
    print(f"Final URL:      {final_url}")
    print(f"Remote size:    {human_size(size)}")
    print(f"Range support:  {range_supported}")
    print(f"Drive folder:   {DRIVE_OUTPUT_DIR}")
    print(f"Local work dir: {LOCAL_WORK_DIR}")

    if size is not None and size <= MAX_SINGLE_FILE_BYTES:
        print("\nFile is 10 GB or smaller. Downloading as a single file.")

        local_file = Path(LOCAL_WORK_DIR) / filename
        download_whole_to_file(
            session=session,
            url=final_url,
            local_path=local_file,
            expected_size=size,
        )

        upload_to_drive(local_file, DRIVE_OUTPUT_DIR)

        if DELETE_LOCAL_AFTER_UPLOAD:
            local_file.unlink(missing_ok=True)
            partial_path = Path(str(local_file) + ".partial")
            partial_path.unlink(missing_ok=True)
            print(f"Deleted local file to free space: {local_file}")

    elif size is not None and size > MAX_SINGLE_FILE_BYTES:
        process_large_file_in_parts(
            session=session,
            url=final_url,
            filename=filename,
            size=size,
            range_supported=range_supported,
        )

    else:
        print("\nRemote size is unknown.")
        print("You can still download it as one file, but the script cannot know whether it exceeds 10 GB.")
        print("For large unknown-size files, use a direct URL that returns Content-Length or supports Content-Range.")

        if yes_no("Download as a single file anyway?", default=False):
            local_file = Path(LOCAL_WORK_DIR) / filename
            download_whole_to_file(
                session=session,
                url=final_url,
                local_path=local_file,
                expected_size=None,
            )

            upload_to_drive(local_file, DRIVE_OUTPUT_DIR)

            if DELETE_LOCAL_AFTER_UPLOAD:
                local_file.unlink(missing_ok=True)
                partial_path = Path(str(local_file) + ".partial")
                partial_path.unlink(missing_ok=True)
                print(f"Deleted local file to free space: {local_file}")
        else:
            print("Stopped.")


main()
