[English](README.md)

# ColabDownloader

این مخزن شامل اسکریپت‌های مستقل برای Google Colab است که یک فایل را از یک URL دانلود می‌کنند و سپس آن را داخل Google Drive کپی می‌کنند.

در این مخزن دو روش وجود دارد:

- `ColabDownloader.py`: فایل را دانلود می‌کند و آن را یا به‌صورت یک فایل واحد، یا به‌صورت چند فایل بزرگ با نام‌های `.part####` داخل Drive قرار می‌دهد.
- `ColabChunkedDownloader.py`: فایل را به تعداد زیادی chunk کوچک تقسیم می‌کند و آن‌ها را در پوشه‌های `split_####` قرار می‌دهد تا انتقال مرحله‌ای از Colab به Drive راحت‌تر شود.

همچنین یک نسخه‌ی مبتنی بر `yt-dlp` هم دارد:

- `ColabYTDownloader.py`: یک آیتم رسانه‌ای را با `yt-dlp` دانلود می‌کند و سپس آن را یا به‌صورت یک فایل واحد، یا به‌صورت چند فایل بزرگ با نام‌های `.part####` داخل Drive قرار می‌دهد.

## کدام اسکریپت مناسب‌تر است؟

- وقتی ساده‌ترین مسیر را می‌خواهید و سرور مبدأ از HTTP Range پشتیبانی می‌کند، از `ColabDownloader.py` استفاده کنید.
- وقتی فایل خیلی بزرگ است و می‌خواهید آن را در چند مرحله‌ی حدود `10 GB` از طریق Drive جابه‌جا کنید، از `ColabChunkedDownloader.py` استفاده کنید.
- وقتی مبدأ یک صفحه‌ی ویدئو یا رسانه‌ی پشتیبانی‌شده توسط `yt-dlp` است و workflow فایل واحد یا partها را می‌خواهید، از `ColabYTDownloader.py` استفاده کنید.

## اجرای اسکریپت‌ها در Google Colab

این اسکریپت‌ها مخصوص Colab نوشته شده‌اند و خودشان با `drive.mount("/content/drive")` گوگل‌درایو را mount می‌کنند.

### مراحل کلی

1. یک نوت‌بوک جدید در `https://colab.research.google.com/` باز کنید.
2. محتوای کامل هر اسکریپتی را که می‌خواهید اجرا کنید (`ColabDownloader.py`، `ColabChunkedDownloader.py` یا `ColabYTDownloader.py`) داخل یک سلول کد کپی کنید.
3. تنظیمات ابتدای فایل را ویرایش کنید.
4. سلول را اجرا کنید.
5. وقتی Colab درخواست دسترسی به Google Drive داد، آن را تأیید کنید.
6. پیام‌ها و پرسش‌های خود اسکریپت را دنبال کنید.

اسکریپت مبتنی بر `yt-dlp` اگر لازم باشد، `yt-dlp` و `ffmpeg` را داخل Colab به‌صورت خودکار نصب می‌کند.

هر سه اسکریپت حالا از یک الگوی یکسان پیروی می‌کنند:

- قبل از این‌که فایل، part یا split فعلی را کامل آپلود کنند، سوالی نمی‌پرسند.
- اگر workflow مبدأ اجازه بدهد، دانلود را با چند اتصال انجام می‌دهند و در غیر این صورت به یک اتصال برمی‌گردند.
- بعد از پایان آپلود فعلی، فایل‌های موقت محلی را حذف می‌کنند.
- بعد از آن می‌پرسند آیا نسخه‌ی فعلی موجود در Drive را دانلود کرده‌اید تا بتوانند آن را از Drive پاک کنند.
- اگر تأیید کنید، نسخه‌ی فعلی را از Drive حذف می‌کنند و سپس می‌پرسند آیا می‌خواهید part یا split بعدی دانلود شود یا نه.

### تنظیمات مهم در `ColabDownloader.py`

- `URL`: آدرس مستقیم فایلی که باید دانلود شود.
- `DRIVE_OUTPUT_DIR`: مسیر ذخیره‌ی فایل یا partها در Google Drive.
- `LOCAL_WORK_DIR`: مسیر موقت داخل ماشین Colab.
- `MAX_SINGLE_FILE_BYTES`: مرز بین حالت فایل واحد و حالت چند part.
- `PART_SIZE_BYTES`: اندازه‌ی هر فایل `.part####`.
- `START_PART_INDEX`: شماره‌ی part شروع در حالت فایل بزرگ.
- `PART_DOWNLOAD_CONNECTIONS`: تعداد اتصال‌های موازی HTTP وقتی سرور از Range requests پشتیبانی می‌کند.

مسیر پیش‌فرض خروجی:

```python
DRIVE_OUTPUT_DIR = "/content/drive/MyDrive/ColabDownloads"
```

### تنظیمات مهم در `ColabChunkedDownloader.py`

- `URL`: آدرس مستقیم فایلی که باید دانلود شود.
- `SPLIT_PART_INDEX`: شماره‌ی split شروع را مشخص می‌کند؛ بعد از هر split خود اسکریپت می‌تواند ادامه بدهد.
- `DRIVE_OUTPUT_ROOT`: پوشه‌ی ریشه در Google Drive برای batchها.
- `LOCAL_WORK_ROOT`: مسیر موقت داخل ماشین Colab.
- `CHUNK_SIZE_BYTES`: اندازه‌ی هر chunk.
- `MAX_LOCAL_BATCH_BYTES`: بیشترین حجم محلی هر batch قبل از اجرای batch بعدی.
- `SPLIT_DOWNLOAD_CONNECTIONS`: تعداد Range requestهای موازی HTTP برای split فعلی.
- `ENABLE_REDOWNLOAD`: اگر `False` باشد، chunkهای محلی و Drive که اندازه‌ی درست دارند دوباره دانلود نمی‌شوند؛ اگر `True` باشد، split فعلی از اول دوباره دانلود و آپلود می‌شود.

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
- `START_PART_INDEX`: شماره‌ی part شروع در حالت فایل بزرگ.
- `YT_DLP_CONCURRENT_FRAGMENTS`: تعداد fragment downloadهای هم‌زمان در `yt-dlp` وقتی مبدأ از آن پشتیبانی کند.

مسیر پیش‌فرض خروجی:

```python
DRIVE_OUTPUT_DIR = "/content/drive/MyDrive/ColabYTDownloads"
```

## هر اسکریپت چه چیزی را داخل Google Drive آپلود می‌کند؟

### `ColabDownloader.py`

- اگر فایل مبدأ `10 GiB` یا کوچک‌تر باشد، یک فایل واحد داخل `DRIVE_OUTPUT_DIR` قرار می‌دهد.
- اگر فایل از `MAX_SINGLE_FILE_BYTES` بزرگ‌تر باشد، آن را از `START_PART_INDEX` به بعد به partهایی با نام‌هایی مثل `filename.part0001-of-0004` پردازش می‌کند.
- هر part بزرگ ابتدا دانلود می‌شود، و اگر HTTP Range پشتیبانی شود با چند اتصال دریافت می‌شود، سپس همراه با `filename.manifest.json` آپلود می‌شود.
- بعد از هر آپلود، اسکریپت فایل‌های موقت محلی را حذف می‌کند، می‌پرسد آیا نسخه‌ی موجود در Drive را دانلود کرده‌اید، در صورت تأیید آن نسخه را از Drive پاک می‌کند و در صورت تمایل به part بعدی می‌رود.
- این manifest شامل نام فایل اصلی، اندازه‌ی اصلی، تعداد partها و ترتیب بازسازی است.

حالت فایل‌های بزرگ نیاز دارد که سرور مبدأ از HTTP Range پشتیبانی کند تا Colab بتواند هر part را جداگانه دانلود کند و از بایت `0` دوباره شروع نکند.

### `ColabChunkedDownloader.py`

- اسکریپت از `SPLIT_PART_INDEX` شروع می‌کند و می‌تواند به‌صورت خودکار به splitهای بعدی ادامه بدهد.
- با تنظیمات پیش‌فرض، هر split شامل حداکثر `1000` chunk با اندازه‌ی `10 MB` است؛ یعنی حدود `10 GB` در هر batch.
- خروجی Drive در مسیری مانند زیر ساخته می‌شود:

```text
/content/drive/MyDrive/ColabChunkDownloads/<safe_filename>/split_0001
```

- داخل این پوشه فایل‌های chunk و همین‌طور یک manifest برای همان split قرار می‌گیرد؛ مثل `filename.split_0001.manifest.json`.
- خود دانلود split به‌صورت بهینه در سطح split انجام می‌شود، نه در سطح هر chunk: اسکریپت بازه‌ی بایتی split را بین چند worker تقسیم می‌کند، هر worker فقط یک HTTP Range request می‌فرستد و همان stream را مستقیم داخل chunkهای نهایی می‌نویسد.
- اگر یک فایل chunk محلی از قبل با اندازه‌ی درست وجود داشته باشد، دوباره دانلود نمی‌شود مگر این‌که `ENABLE_REDOWNLOAD = True` باشد.
- اگر اندازه‌ی یک chunk محلی درست نباشد، اسکریپت هر جا ممکن باشد برای اولین chunk ناقص در هر worker یک semi-resume انجام می‌دهد و اگر ممکن نباشد، داده‌ی آن chunk را دوباره دانلود می‌کند.
- بعد از هر آپلود split، اسکریپت فایل‌های موقت محلی را حذف می‌کند، می‌پرسد آیا آن split را از Drive دانلود کرده‌اید، در صورت تأیید آن split را از Drive پاک می‌کند و در صورت تمایل به split بعدی می‌رود.

### `ColabYTDownloader.py`

- یک آیتم را با `yt-dlp` و با استفاده از کوکی‌ها و تنظیمات کیفیت انتخاب‌شده دانلود می‌کند.
- اگر فایل رسانه‌ای دانلودشده از مرز تعیین‌شده کوچک‌تر باشد، یک فایل واحد داخل `DRIVE_OUTPUT_DIR` آپلود می‌کند.
- اگر فایل رسانه‌ای دانلودشده از `MAX_SINGLE_FILE_BYTES` بزرگ‌تر باشد، آن را از `START_PART_INDEX` به بعد در قالب فایل‌هایی با الگوی `filename.part0001-of-0004` پردازش می‌کند.
- مرحله‌ی دانلود رسانه به خود `yt-dlp` سپرده می‌شود.
- `yt-dlp` وقتی سایت و فرمت انتخاب‌شده اجازه بدهند، از fragment download هم‌زمان استفاده می‌کند.
- بعد از هر آپلود فایل بزرگ، اسکریپت part موقت محلی را حذف می‌کند، می‌پرسد آیا نسخه‌ی موجود در Drive را دانلود کرده‌اید، در صورت تأیید آن نسخه را از Drive پاک می‌کند و در صورت تمایل به part بعدی می‌رود.

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

### 5. در صورت نیاز از fork مخصوص domain fronting برای `rclone` استفاده کنید

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

برای دانلود `ColabChunkDownloads` یا `ColabYTDownloads` هم می‌توانید همین flagها را کنار همان دستورهای `copy` بالا استفاده کنید.

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

### 3. دانلود پوشه‌ی اشتراکیِ خروجی `ColabChunkedDownloader.py`

پوشه‌های عمومی share شده را می‌توان مستقیم دانلود کرد:

```bash
gdrivedl -u 'https://drive.google.com/drive/folders/FOLDER_ID?usp=sharing'
```

این روش برای پوشه‌های `split_0001`، `split_0002` و بقیه‌ی batchها بسیار مناسب است.

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

### بازسازی chunkهای `ColabChunkedDownloader.py`

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
- فایل‌های part و chunk را تغییر نام ندهید، مگر اینکه ترتیب آن‌ها را دقیق حفظ کنید.

## مجوز استفاده

MIT
