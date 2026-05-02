[English](README.md)

# ColabDownloader

این مخزن شامل اسکریپت‌های مستقل برای Google Colab است که یک فایل را از یک URL دانلود می‌کنند و سپس آن را داخل Google Drive کپی می‌کنند.

در این مخزن دو روش وجود دارد:

- `ColabDownloader.py`: فایل را دانلود می‌کند و آن را یا به‌صورت یک فایل واحد، یا به‌صورت چند فایل بزرگ با نام‌های `.part####` داخل Drive قرار می‌دهد.
- `ColabChunkedDownloader.py`: فایل را به تعداد زیادی chunk کوچک تقسیم می‌کند و آن‌ها را در پوشه‌های `split_####` قرار می‌دهد تا انتقال مرحله‌ای از Colab به Drive راحت‌تر شود.

همچنین دو نسخه‌ی مبتنی بر `yt-dlp` هم دارد:

- `ColabYTDownloader.py`: یک آیتم رسانه‌ای را با `yt-dlp` دانلود می‌کند و سپس آن را یا به‌صورت یک فایل واحد، یا به‌صورت چند فایل بزرگ با نام‌های `.part####` داخل Drive قرار می‌دهد.
- `ColabChunkedYTDownloader.py`: یک آیتم رسانه‌ای را با `yt-dlp` دانلود می‌کند و سپس آن را به‌صورت chunkهایی در پوشه‌های `split_####` داخل Drive قرار می‌دهد.

## کدام اسکریپت مناسب‌تر است؟

- وقتی ساده‌ترین مسیر را می‌خواهید و سرور مبدأ از HTTP Range پشتیبانی می‌کند، از `ColabDownloader.py` استفاده کنید.
- وقتی فایل خیلی بزرگ است و می‌خواهید آن را در چند مرحله‌ی حدود `10 GB` از طریق Drive جابه‌جا کنید، از `ColabChunkedDownloader.py` استفاده کنید.
- وقتی مبدأ یک صفحه‌ی ویدئو یا رسانه‌ی پشتیبانی‌شده توسط `yt-dlp` است و workflow فایل واحد یا partها را می‌خواهید، از `ColabYTDownloader.py` استفاده کنید.
- وقتی مبدأ یک صفحه‌ی ویدئو یا رسانه‌ی پشتیبانی‌شده توسط `yt-dlp` است و workflow پوشه‌های chunk در Drive را می‌خواهید، از `ColabChunkedYTDownloader.py` استفاده کنید.

## اجرای اسکریپت‌ها در Google Colab

این اسکریپت‌ها مخصوص Colab نوشته شده‌اند و خودشان با `drive.mount("/content/drive")` گوگل‌درایو را mount می‌کنند.

### مراحل کلی

1. یک نوت‌بوک جدید در `https://colab.research.google.com/` باز کنید.
2. محتوای کامل هر اسکریپتی را که می‌خواهید اجرا کنید (`ColabDownloader.py`، `ColabChunkedDownloader.py`، `ColabYTDownloader.py` یا `ColabChunkedYTDownloader.py`) داخل یک سلول کد کپی کنید.
3. تنظیمات ابتدای فایل را ویرایش کنید.
4. سلول را اجرا کنید.
5. وقتی Colab درخواست دسترسی به Google Drive داد، آن را تأیید کنید.
6. پیام‌ها و پرسش‌های خود اسکریپت را دنبال کنید.

دو اسکریپت مبتنی بر `yt-dlp` اگر لازم باشد، `yt-dlp` و `ffmpeg` را داخل Colab به‌صورت خودکار نصب می‌کنند.

### تنظیمات مهم در `ColabDownloader.py`

- `URL`: آدرس مستقیم فایلی که باید دانلود شود.
- `DRIVE_OUTPUT_DIR`: مسیر ذخیره‌ی فایل یا partها در Google Drive.
- `LOCAL_WORK_DIR`: مسیر موقت داخل ماشین Colab.
- `MAX_SINGLE_FILE_BYTES`: مرز بین حالت فایل واحد و حالت چند part.
- `PART_SIZE_BYTES`: اندازه‌ی هر فایل `.part####`.

مسیر پیش‌فرض خروجی:

```python
DRIVE_OUTPUT_DIR = "/content/drive/MyDrive/ColabDownloads"
```

### تنظیمات مهم در `ColabChunkedDownloader.py`

- `URL`: آدرس مستقیم فایلی که باید دانلود شود.
- `SPLIT_PART_INDEX`: مشخص می‌کند در این اجرا کدام batch ساخته شود.
- `DRIVE_OUTPUT_ROOT`: پوشه‌ی ریشه در Google Drive برای batchها.
- `LOCAL_WORK_ROOT`: مسیر موقت داخل ماشین Colab.
- `CHUNK_SIZE_BYTES`: اندازه‌ی هر chunk.
- `MAX_LOCAL_BATCH_BYTES`: بیشترین حجم محلی هر batch قبل از اجرای batch بعدی.

مسیر پیش‌فرض خروجی:

```python
DRIVE_OUTPUT_ROOT = "/content/drive/MyDrive/ColabChunkDownloads"
```

### تنظیمات مهم در `ColabYTDownloader.py`

- `URL`: آدرس صفحه‌ی ویدئو یا رسانه‌ای که `yt-dlp` از آن پشتیبانی می‌کند.
- `COOKIES_TEXT`: کوکی اختیاری که می‌توانید آن را یا در قالب فایل کوکی Netscape و یا به شکل یک رشته‌ی معمولی `Cookie:` وارد کنید.
- `QUALITY`: سقف اختیاری برای ارتفاع ویدئو مثل `720` یا `1080`؛ اگر `None` بماند، بهترین کیفیت موجود انتخاب می‌شود.
- `YT_DLP_FORMAT`: فرمت‌سلکتور خام `yt-dlp` به‌صورت اختیاری؛ اگر آن را تنظیم کنید، به `QUALITY` اولویت دارد.
- `MERGE_OUTPUT_FORMAT`: فرمت نهایی کانتینر بعد از merge، مثلا `mp4`.
- `DRIVE_OUTPUT_DIR`: مسیر ذخیره‌ی فایل یا partها در Google Drive.
- `LOCAL_WORK_DIR`: مسیر موقت داخل ماشین Colab.

مسیر پیش‌فرض خروجی:

```python
DRIVE_OUTPUT_DIR = "/content/drive/MyDrive/ColabYTDownloads"
```

### تنظیمات مهم در `ColabChunkedYTDownloader.py`

- `URL`: آدرس صفحه‌ی ویدئو یا رسانه‌ای که `yt-dlp` از آن پشتیبانی می‌کند.
- `COOKIES_TEXT`: کوکی اختیاری که می‌توانید آن را یا در قالب فایل کوکی Netscape و یا به شکل یک رشته‌ی معمولی `Cookie:` وارد کنید.
- `QUALITY`: سقف اختیاری برای ارتفاع ویدئو مثل `720` یا `1080`؛ اگر `None` بماند، بهترین کیفیت موجود انتخاب می‌شود.
- `YT_DLP_FORMAT`: فرمت‌سلکتور خام `yt-dlp` به‌صورت اختیاری؛ اگر آن را تنظیم کنید، به `QUALITY` اولویت دارد.
- `SPLIT_PART_INDEX`: مشخص می‌کند در این اجرا کدام batch ساخته شود.
- `DRIVE_OUTPUT_ROOT`: پوشه‌ی ریشه در Google Drive برای batchهای chunk.
- `LOCAL_WORK_ROOT`: مسیر موقت داخل ماشین Colab.

مسیر پیش‌فرض خروجی:

```python
DRIVE_OUTPUT_ROOT = "/content/drive/MyDrive/ColabChunkYTDownloads"
```

## هر اسکریپت چه چیزی را داخل Google Drive آپلود می‌کند؟

### `ColabDownloader.py`

- اگر فایل مبدأ `10 GiB` یا کوچک‌تر باشد، یک فایل واحد داخل `DRIVE_OUTPUT_DIR` قرار می‌دهد.
- اگر فایل از `MAX_SINGLE_FILE_BYTES` بزرگ‌تر باشد، آن را به partهایی با نام‌هایی مثل `filename.part0001-of-0004` تقسیم می‌کند.
- بعد از آپلود partهای انتخاب‌شده، فایل `filename.manifest.json` را هم آپلود می‌کند.
- این manifest شامل نام فایل اصلی، اندازه‌ی اصلی، تعداد partها و ترتیب بازسازی است.

حالت فایل‌های بزرگ نیاز دارد که سرور مبدأ از HTTP Range پشتیبانی کند تا Colab بتواند هر part را جداگانه دانلود کند و از بایت `0` دوباره شروع نکند.

### `ColabChunkedDownloader.py`

- هر اجرا فقط یک `SPLIT_PART_INDEX` را پردازش می‌کند.
- با تنظیمات پیش‌فرض، هر split شامل حداکثر `1000` chunk با اندازه‌ی `10 MB` است؛ یعنی حدود `10 GB` در هر batch.
- خروجی Drive در مسیری مانند زیر ساخته می‌شود:

```text
/content/drive/MyDrive/ColabChunkDownloads/<safe_filename>/split_0001
```

- داخل این پوشه فایل‌های chunk و همین‌طور یک manifest برای همان split قرار می‌گیرد؛ مثل `filename.split_0001.manifest.json`.
- بعد از پایان آپلود، اسکریپت مکث می‌کند تا آن پوشه‌ی Drive را دانلود یا بررسی کنید و بعد سراغ split بعدی بروید.

چرخه‌ی معمول برای فایل‌های خیلی بزرگ:

1. `SPLIT_PART_INDEX = 1` را تنظیم کنید.
2. سلول را اجرا کنید و صبر کنید تا آپلود روی Drive تمام شود.
3. پوشه‌ی ایجادشده در Drive را با `rclone` یا `gdrivedl` روی سیستم خودتان دانلود کنید.
4. به Colab برگردید، تأیید کنید، سپس `SPLIT_PART_INDEX = 2` را بگذارید.
5. این روند را تا پایان همه‌ی splitها تکرار کنید.

### `ColabYTDownloader.py`

- یک آیتم را با `yt-dlp` و با استفاده از کوکی‌ها و تنظیمات کیفیت انتخاب‌شده دانلود می‌کند.
- اگر فایل رسانه‌ای دانلودشده از مرز تعیین‌شده کوچک‌تر باشد، یک فایل واحد داخل `DRIVE_OUTPUT_DIR` آپلود می‌کند.
- اگر فایل رسانه‌ای دانلودشده از `MAX_SINGLE_FILE_BYTES` بزرگ‌تر باشد، آن را به فایل‌هایی با الگوی `filename.part0001-of-0004` تقسیم می‌کند و همراه با manifest آپلود می‌کند.

### `ColabChunkedYTDownloader.py`

- یک آیتم را با `yt-dlp` و با استفاده از کوکی‌ها و تنظیمات کیفیت انتخاب‌شده دانلود می‌کند.
- سپس برای `SPLIT_PART_INDEX` فعلی، فایل‌های `chunk########-of-########` می‌سازد و همان پوشه‌ی split را داخل Drive آپلود می‌کند.
- همچنین یک manifest برای همان split مثل `filename.split_0001.manifest.json` آپلود می‌کند.

نکته‌ی مهم درباره‌ی workflow chunked مبتنی بر `yt-dlp`:

- `ColabChunkedYTDownloader.py` ابتدا کل فایل رسانه‌ای را داخل ماشین Colab دانلود می‌کند و فقط بعد از آن chunkها را می‌سازد.
- یعنی خود ماشین Colab همچنان باید به‌اندازه‌ی کل فایل رسانه‌ای فضای محلی کافی داشته باشد.

## دانلود فایل‌های آپلودشده از Google Drive بعد از پایان اجرای Colab

دو روش عملی دارید:

- `rclone`: بهترین گزینه برای دانلود مستقیم از Google Drive شخصی خودتان.
- `gdrivedl`: بهترین گزینه وقتی می‌خواهید با لینک اشتراکی Google Drive دانلود انجام دهید.

## روش اول: دانلود از Google Drive با `rclone`

وقتی فایل‌ها هنوز در `MyDrive` شخصی شما هستند و نمی‌خواهید آن‌ها را عمومی کنید، `rclone` بهترین انتخاب است.

### 1. ساخت remote برای Google Drive

بعد از نصب `rclone` این دستور را اجرا کنید:

```bash
rclone config
```

یک remote مثلا با نام `mydrive` بسازید، نوع آن را `drive` انتخاب کنید و مراحل OAuth را در مرورگر کامل کنید.

### 2. دانلود خروجی `ColabDownloader.py`

اگر پوشه‌ی پیش‌فرض را تغییر نداده‌اید:

```bash
rclone copy 'mydrive:ColabDownloads' ./ColabDownloads -P
```

برای دانلود فقط یک فایل یا یک part مشخص:

```bash
rclone copy 'mydrive:ColabDownloads/your-file.part0001-of-0004' . -P
```

### 3. دانلود خروجی `ColabChunkedDownloader.py`

برای دانلود یک پوشه‌ی split:

```bash
rclone copy 'mydrive:ColabChunkDownloads/your-file/split_0001' ./split_0001 -P
```

برای دانلود کل ساختار chunkها:

```bash
rclone copy 'mydrive:ColabChunkDownloads/your-file' ./your-file -P
```

چند دستور کمکی:

```bash
rclone lsf 'mydrive:ColabDownloads'
rclone lsf 'mydrive:ColabChunkDownloads/your-file'
```

### 4. دانلود خروجی `ColabYTDownloader.py`

اگر پوشه‌ی پیش‌فرض را تغییر نداده‌اید:

```bash
rclone copy 'mydrive:ColabYTDownloads' ./ColabYTDownloads -P
```

برای دانلود فقط یک فایل رسانه‌ای یا یک part مشخص:

```bash
rclone copy 'mydrive:ColabYTDownloads/your-video.part0001-of-0004' . -P
```

### 5. دانلود خروجی `ColabChunkedYTDownloader.py`

برای دانلود یک پوشه‌ی split:

```bash
rclone copy 'mydrive:ColabChunkYTDownloads/your-video/split_0001' ./split_0001 -P
```

برای دانلود کل ساختار chunkها:

```bash
rclone copy 'mydrive:ColabChunkYTDownloads/your-video' ./your-video -P
```

چند دستور کمکی:

```bash
rclone lsf 'mydrive:ColabYTDownloads'
rclone lsf 'mydrive:ColabChunkYTDownloads/your-video'
```

### 6. در صورت نیاز از fork مخصوص domain fronting برای `rclone` استفاده کنید

اگر روی شبکه‌ی شما دسترسی مستقیم به Google Drive یا Google APIs فیلتر می‌شود، می‌توانید از این fork که از domain fronting پشتیبانی می‌کند استفاده کنید: `https://github.com/aleskxyz/rclone`

ساده‌ترین روش build کردن آن:

```bash
git clone https://github.com/aleskxyz/rclone
cd rclone
go build
```

بعد همان دستورهای `copy` بالا را اجرا کنید، اما flagهای fronting را هم اضافه کنید.

flagهای مهم:

- `--fronting-enable`: domain fronting را در shared HTTP transport فعال می‌کند.
- `--fronting-target google.com`: مقصد dial شبکه را `google.com` قرار می‌دهد.
- `--fronting-sni google.com`: مقدار TLS SNI را `google.com` می‌گذارد. اگر آن را ننویسید، خود fork به‌صورت پیش‌فرض همان `--fronting-target` را برای SNI استفاده می‌کند.
- `--fronting-domains '*.googleapis.com,drive.google.com'`: فقط requestهایی را front می‌کند که hostname آن‌ها match شود. هم hostname دقیق و هم wildcard با الگوی `*.` پشتیبانی می‌شود و بقیه‌ی hostها بدون fronting می‌مانند.

مثال:

```bash
./rclone copy 'mydrive:ColabDownloads' ./ColabDownloads -P \
  --fronting-enable \
  --fronting-target google.com \
  --fronting-sni google.com \
  --fronting-domains '*.googleapis.com,*.google.com,*.googleusercontent.com'
```

برای دانلود `ColabChunkDownloads`، `ColabYTDownloads` یا `ColabChunkYTDownloads` هم می‌توانید همین flagها را کنار همان دستورهای `copy` بالا استفاده کنید.

این‌که domain fronting واقعا کار کند، به وضعیت فعلی مسیر شبکه و رفتار edgeهای گوگل بستگی دارد.

## روش دوم: دانلود با لینک اشتراکی و `gdrivedl`

وقتی می‌خواهید به‌جای اتصال مستقیم `rclone` به Drive شخصی خودتان، دانلود را با لینک اشتراکی انجام دهید، از `gdrivedl` استفاده کنید.

نکات مهم:

- `gdrivedl` با لینک‌های اشتراکی Google Drive کار می‌کند.
- اگر فایل فقط در `MyDrive` خصوصی شماست و share نشده، بهتر است از `rclone` استفاده کنید.
- برای استفاده از `gdrivedl` باید فایل یا پوشه‌ی خروجی را از داخل Drive با حالت `Anyone with the link` و دسترسی `Viewer` به اشتراک بگذارید.

### 1. اشتراک‌گذاری فایل یا پوشه‌ی آپلودشده در Drive

در Google Drive:

1. روی فایل یا پوشه‌ی مورد نظر راست‌کلیک کنید.
2. گزینه‌ی `Share` را بزنید.
3. سطح دسترسی را روی `Anyone with the link` قرار دهید.
4. مجوز را روی `Viewer` نگه دارید.
5. لینک ایجادشده را کپی کنید.

### 2. نصب یا build کردن `gdrivedl`

اگر مخزن همسایه‌ای که برای این پروژه اشاره شده را دارید:

```bash
cd ../gdrivedl
go build
```

بعد آن را به این شکل اجرا کنید:

```bash
./gdrivedl -u 'https://drive.google.com/drive/folders/FOLDER_ID?usp=sharing'
```

یا مستقیماً آن را نصب کنید:

```bash
go install github.com/hadi77ir/gdrivedl@latest
```

### 3. دانلود پوشه‌ی اشتراکیِ خروجی `ColabChunkedDownloader.py` یا `ColabChunkedYTDownloader.py`

پوشه‌های عمومی share شده را می‌توان مستقیم دانلود کرد:

```bash
gdrivedl -u 'https://drive.google.com/drive/folders/FOLDER_ID?usp=sharing'
```

این روش برای پوشه‌های `split_0001`، `split_0002` و بقیه‌ی batchهای هر دو workflow chunked بسیار مناسب است.

### 4. دانلود فایل اشتراکی یا partهای خروجی `ColabDownloader.py` یا `ColabYTDownloader.py`

```bash
gdrivedl -u 'https://drive.google.com/file/d/FILE_ID/view?usp=sharing'
```

برای فایل‌های خیلی بزرگ، حالت resumable مفید است:

```bash
gdrivedl -u 'https://drive.google.com/file/d/FILE_ID/view?usp=sharing' -r 100m
```

نکته‌ها:

- برای پوشه‌های عمومی اشتراکی، API key لازم نیست.
- برای دانلود resumable و بعضی metadata lookupها، `gdrivedl` از `--apikey` یا `GDRIVEDL_APIKEY` هم پشتیبانی می‌کند.
- مخزن پروژه: `https://github.com/hadi77ir/gdrivedl`

### 5. استفاده از domain fronting در `gdrivedl`

`gdrivedl` هم از domain fronting پشتیبانی می‌کند. اگر می‌خواهید `google.com` به‌عنوان fronting target استفاده شود، این flagها را اضافه کنید:

- `--fronting-enable`
- `--fronting-target google.com`
- `--fronting-sni google.com`

مثال:

```bash
gdrivedl \
  --fronting-enable \
  --fronting-target google.com \
  --fronting-sni google.com \
  --utls-profile firefox_auto \
  -u 'https://drive.google.com/file/d/FILE_ID/view?usp=sharing'
```

همین flagها برای دانلود پوشه‌های اشتراکی هم قابل استفاده‌اند.

برخلاف fork مربوط به `rclone` در بالا، `gdrivedl` فیلتر `--fronting-domains` ندارد؛ وقتی fronting را فعال کنید، روی requestهای shared transport آن اعمال می‌شود.

این‌که domain fronting واقعا کار کند، به وضعیت فعلی مسیر شبکه و رفتار edgeهای گوگل بستگی دارد.

## بازسازی فایل بعد از دانلود روی سیستم خودتان

### بازسازی partهای `ColabDownloader.py` یا `ColabYTDownloader.py`

بعد از دانلود همه‌ی فایل‌های `.part####` و فایل manifest، آن‌ها را داخل یک پوشه قرار دهید و به ترتیب به هم بچسبانید.

در Linux یا macOS:

```bash
cat your-file.part0001-of-0004 your-file.part0002-of-0004 your-file.part0003-of-0004 your-file.part0004-of-0004 > your-file
```

اگر درباره‌ی ترتیب مطمئن نبودید، از فایل `*.manifest.json` استفاده کنید.

### بازسازی chunkهای `ColabChunkedDownloader.py` یا `ColabChunkedYTDownloader.py`

بعد از دانلود همه‌ی پوشه‌های `split_####`، همه‌ی chunkها را داخل یک مسیر قرار دهید و فایل اصلی را بازسازی کنید.

در Linux یا macOS:

```bash
cat your-file.chunk*-of-00001234 > your-file
```

شماره‌ی chunkها با صفر از چپ پر شده است، پس ترتیب الفبایی همان ترتیب صحیح بازسازی است.

## توصیه‌های عملی

- اگر خروجی در Drive شخصی شما خصوصی می‌ماند، `rclone` بهترین انتخاب است.
- اگر به domain fronting با target برابر `google.com` نیاز دارید، از `gdrivedl` یا fork مربوط به `https://github.com/aleskxyz/rclone` استفاده کنید.
- اگر می‌خواهید دانلود را با لینک اشتراکی انجام دهید، `gdrivedl` گزینه‌ی تمیزتری است.
- برای فایل‌های خیلی بزرگ، `ColabChunkedDownloader.py` امن‌تر است چون هر اجرا فقط یک batch هم‌اندازه با Drive را مدیریت می‌کند.
- از `ColabChunkedYTDownloader.py` فقط وقتی استفاده کنید که ماشین Colab برای خود فایل رسانه‌ای کامل، قبل از chunk شدن، فضای محلی کافی داشته باشد.
- فایل‌های part و chunk را تغییر نام ندهید، مگر اینکه ترتیب آن‌ها را دقیق حفظ کنید.

## مجوز استفاده

MIT
