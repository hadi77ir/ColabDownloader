[فارسی](README.fa.md)

# ColabDownloader

Small standalone Google Colab scripts for downloading a file from a URL and copying it into Google Drive.

This repo contains two workflows:

- `ColabDownloader.py`: downloads one file and uploads it to Drive as either a single file or a few large `.part####` files.
- `ColabChunkedDownloader.py`: downloads one file as many small chunk files, grouped into `split_####` folders that are sized for repeated Colab-to-Drive batches.

## Which script should you use?

- Use `ColabDownloader.py` when you want the simplest workflow and the source server supports HTTP Range requests.
- Use `ColabChunkedDownloader.py` when the file is very large and you want to move it through Drive in repeated batches of about `10 GB` each.

## Running the scripts in Google Colab

These scripts are already written for Colab and mount Google Drive themselves with `drive.mount("/content/drive")`.

### Basic steps

1. Open a new notebook at `https://colab.research.google.com/`.
2. Copy the full contents of either `ColabDownloader.py` or `ColabChunkedDownloader.py` into one code cell.
3. Edit the settings at the top of the script.
4. Run the cell.
5. Approve Google Drive access when Colab asks.
6. Follow the prompts printed by the script.

### Settings to edit in `ColabDownloader.py`

- `URL`: direct URL to the file you want to fetch.
- `DRIVE_OUTPUT_DIR`: where the uploaded file or parts should be stored in Drive.
- `LOCAL_WORK_DIR`: temporary working directory inside the Colab VM.
- `MAX_SINGLE_FILE_BYTES`: threshold for deciding between single-file mode and split-part mode.
- `PART_SIZE_BYTES`: size of each uploaded `.part####` file.

Default output location:

```python
DRIVE_OUTPUT_DIR = "/content/drive/MyDrive/ColabDownloads"
```

### Settings to edit in `ColabChunkedDownloader.py`

- `URL`: direct URL to the file you want to fetch.
- `SPLIT_PART_INDEX`: which Drive batch to generate on this run.
- `DRIVE_OUTPUT_ROOT`: root Drive folder for chunk batches.
- `LOCAL_WORK_ROOT`: temporary working directory inside the Colab VM.
- `CHUNK_SIZE_BYTES`: size of each chunk file.
- `MAX_LOCAL_BATCH_BYTES`: maximum local batch size before you download the next batch in another run.

Default output location:

```python
DRIVE_OUTPUT_ROOT = "/content/drive/MyDrive/ColabChunkDownloads"
```

## What each script uploads to Google Drive

### `ColabDownloader.py`

- If the remote file is `10 GiB` or smaller, it uploads one file to `DRIVE_OUTPUT_DIR`.
- If the remote file is larger than `MAX_SINGLE_FILE_BYTES`, it downloads and uploads named parts like `filename.part0001-of-0004`.
- After the selected parts are uploaded, it also uploads `filename.manifest.json`.
- The manifest records the original filename, original size, total part count, and the rebuild order.

Large-file mode needs a server that supports HTTP Range requests so Colab can fetch one part at a time without restarting from byte `0`.

### `ColabChunkedDownloader.py`

- Each run processes exactly one `SPLIT_PART_INDEX`.
- With the defaults, one split part contains up to `1000` chunks of `10 MB` each, so the batch is about `10 GB`.
- Drive output is written to:

```text
/content/drive/MyDrive/ColabChunkDownloads/<safe_filename>/split_0001
```

- That folder contains the chunk files plus a split manifest such as `filename.split_0001.manifest.json`.
- After the upload finishes, the script pauses so you can download or verify that Drive folder before continuing with the next split part.

Typical loop for large files:

1. Set `SPLIT_PART_INDEX = 1`.
2. Run the cell and wait for the Drive upload to finish.
3. Download that Drive folder to your own machine with `rclone` or `gdrivedl`.
4. Return to Colab, confirm the batch, then set `SPLIT_PART_INDEX = 2`.
5. Repeat until all split parts are finished.

## Downloading the uploaded Drive files after the Colab run

You have two practical options:

- `rclone`: best for downloading from your own private Google Drive.
- `gdrivedl`: best when you want to download from a Google Drive share link.

## Option 1: Download from Google Drive with `rclone`

Use `rclone` when the files are still in your own `MyDrive` and you do not want to make them public.

### 1. Configure a Google Drive remote

Install `rclone`, then run:

```bash
rclone config
```

Create a remote such as `mydrive`, choose `drive` as the storage type, and finish the OAuth flow in your browser.

### 2. Download the output of `ColabDownloader.py`

If you kept the default Drive folder:

```bash
rclone copy 'mydrive:ColabDownloads' ./ColabDownloads -P
```

To download only one specific file or part:

```bash
rclone copy 'mydrive:ColabDownloads/your-file.part0001-of-0004' . -P
```

### 3. Download the output of `ColabChunkedDownloader.py`

Download a single split folder:

```bash
rclone copy 'mydrive:ColabChunkDownloads/your-file/split_0001' ./split_0001 -P
```

Download the whole chunked folder tree:

```bash
rclone copy 'mydrive:ColabChunkDownloads/your-file' ./your-file -P
```

Useful helper commands:

```bash
rclone lsf 'mydrive:ColabDownloads'
rclone lsf 'mydrive:ColabChunkDownloads/your-file'
```

## Option 2: Download from a shared link with `gdrivedl`

Use `gdrivedl` when you prefer to download from a Google Drive share link instead of authenticating `rclone` against your own Drive.

Important:

- `gdrivedl` works with Google Drive shared links.
- For files stored only in your private `MyDrive`, use `rclone` instead.
- For `gdrivedl`, first share the uploaded file or folder from Drive with `Anyone with the link` and `Viewer` access.

### 1. Share the uploaded Drive file or folder

In Google Drive:

1. Right-click the uploaded file or folder.
2. Choose `Share`.
3. Set access to `Anyone with the link`.
4. Keep the permission as `Viewer`.
5. Copy the generated link.

### 2. Install or build `gdrivedl`

If you already have the sibling repo mentioned for this project:

```bash
cd ../gdrivedl
go build
```

Then run it with:

```bash
./gdrivedl -u 'https://drive.google.com/drive/folders/FOLDER_ID?usp=sharing'
```

Or install it directly:

```bash
go install github.com/hadi77ir/gdrivedl@latest
```

### 3. Download a shared folder produced by `ColabChunkedDownloader.py`

Public shared folders can be downloaded directly:

```bash
gdrivedl -u 'https://drive.google.com/drive/folders/FOLDER_ID?usp=sharing'
```

That is the most natural match for `split_0001`, `split_0002`, and the other batch folders.

### 4. Download a shared file or one part produced by `ColabDownloader.py`

```bash
gdrivedl -u 'https://drive.google.com/file/d/FILE_ID/view?usp=sharing'
```

For very large shared files, resumable mode is useful:

```bash
gdrivedl -u 'https://drive.google.com/file/d/FILE_ID/view?usp=sharing' -r 100m
```

Notes:

- Public shared folders do not require an API key.
- For resumable downloads and some metadata lookups, `gdrivedl` also supports `--apikey` or `GDRIVEDL_APIKEY`.
- Project repo: `https://github.com/hadi77ir/gdrivedl`

## Rebuilding files after you download them locally

### Rebuild parts from `ColabDownloader.py`

After you download all `.part####` files plus the manifest, place them in one folder and concatenate them in order.

On Linux or macOS:

```bash
cat your-file.part0001-of-0004 your-file.part0002-of-0004 your-file.part0003-of-0004 your-file.part0004-of-0004 > your-file
```

Use the `*.manifest.json` file if you need the exact rebuild order.

### Rebuild chunks from `ColabChunkedDownloader.py`

After you download all `split_####` folders, place all chunk files in one directory and rebuild the original file.

On Linux or macOS:

```bash
cat your-file.chunk*-of-00001234 > your-file
```

The chunk numbers are zero-padded, so normal alphabetical order is the correct rebuild order.

## Practical advice

- Prefer `rclone` if the output stays private in your own Drive.
- Prefer `gdrivedl` if you want a clean shared-link download workflow for a file or folder.
- For very large downloads, `ColabChunkedDownloader.py` is the safer workflow because each run only handles one Drive-sized batch.
- Do not rename part or chunk files unless you preserve their ordering.



## License

MIT
