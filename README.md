
<!-- README.md is generated from README.Rmd. Please edit that file -->

# ucLoader

<!-- badges: start -->
<!-- badges: end -->

Minimal helpers to list, download, upload, and load files stored in
Databricks Unity Catalog (UC) Volumes from R via the Databricks REST
API.

**The core idea:**

1.  Work with UC paths under /Volumes/…
2.  Download to a local temporary file (binary-safe)
3.  Read/load using the appropriate R function
4.  Optionally save locally then upload back to UC (binary-safe
    streaming)

## Features

- Connect/auth check:
  - Validates DATABRICKS_HOST + DATABRICKS_TOKEN (PAT)
  - Performs a lightweight Unity Catalog connectivity check
  - Can prompt interactively for missing credentials (RStudio)
- List files:
  - Lists a UC Volume directory (non-recursive)
  - Returns names, full paths, size (MiB), last modified (UTC)
- Download:
  - Downloads a UC file to a local temporary path
  - Binary-safe (works for .RData, .qs2, .rds, .csv, etc.)
- Upload:
  - Uploads a local file to UC using octet-stream
  - Binary-safe streaming (suitable for large files)
  - Optional overwrite
  - Optional extension enforcement (force destination extension to match
    local file)
- Load/read:
  - `ucload()` supports: .qs2, .RData/.rda, .RDS, .csv
  - For .RData/.rda: loads objects into an environment (like
    `base::load`)
  - For .qs2/.RDS/.csv: reads and assigns a single object into an
    environment using a filename-derived snake_case name
- Save then upload:
  - `ucsave()` writes an object to a temporary file (format chosen by
    extension)
  - Supported formats: .qs2, .RData, .RDS, .csv
  - .RData preserves the original symbol name of the object passed to
    `ucsave()`

## Requirements

1)  Databricks environment variables (PAT-based auth)

- DATABRICKS_HOST

  - Example: <https://adb-xxxxxxxxxxxxxxx.xx.azuredatabricks.net> Notes:
    trailing slashes are removed automatically

- DATABRICKS_TOKEN

  - A Databricks personal access token (PAT)

**Security notes:**

- PAT entry is masked in interactive prompts.
- The token is stored only in the current R process environment.
- ucLoader does not persist tokens to disk.

## Installation

Install from GitHub:

``` r
install.packages("remotes")
remotes::install_github("ridout-m/ucLoader")
library(ucLoader)
```

## Quick start

1)  Validate credentials (interactive prompt if missing)

    ``` r
    connect()
    ```

2)  List files in a UC directory

    ``` r
    list_files("/Volumes/cat/schema/volume/path")
    ```

3)  Download then read manually

    ``` r
    p  <- download("/Volumes/cat/schema/volume/path/data.csv")
    df <- utils::read.csv(p)
    ```

4)  Load/read into your environment:

    ``` r
    #.RData loads objects into your environment (like load())
    ucload("/Volumes/.../snapshot.RData")
    #.qs2/.RDS/.csv assigns a single object using a filename-derived name
    ucload("/Volumes/.../model.qs2")
    ```

5)  Save an object and upload to UC:

    ``` r
    #Default format is .qs2 unless you set extension
    ucsave(mtcars, "/Volumes/.../mtcars.qs2", overwrite = FALSE)
    #Save as .RData while preserving the symbol name
    x <- mtcars 
    ucsave(x, "/Volumes/.../x", extension = ".RData")
    ```

## Key functions

`connect(show_success = TRUE)`

- Ensures DATABRICKS_HOST/DATABRICKS_TOKEN are present and valid.
- Performs a Unity Catalog connectivity check.

`list_files(dir, pattern = NULL, ignore_case = FALSE)`

- Lists files in a UC directory (non-recursive).
- Returns: name, path, file_size_MB, last_modified (UTC POSIXct). -
  Optional regex filtering via pattern.

`download(path)`

- Downloads a UC file to a local temporary path (binary-safe).
- Returns the local temp file path.

`upload(file, path, overwrite = TRUE, force_ext = TRUE)`

- Uploads a local file to a UC destination (binary-safe streaming).
- If force_ext is TRUE, destination extension is forced to match the
  local file.

`ucload(path, ..., envir = parent.frame())`

- Downloads and then loads/reads based on file extension:
  - .RData/.rda: `base::load()` into envir (can create multiple objects)

  - .qs2: `qs2::qs_read()` then assigns into envir

  - .RDS: `base::readRDS()` then assigns into envir

  - .csv: `utils::read.csv()` then assigns into envir (additional args
    via …)

`ucsave(object, path, overwrite = TRUE, extension = ".qs2", force_ext = TRUE, ...)`

- Serialises object to a temp file (by extension) then uploads to UC:
  - .qs2: `qs2::qs_save()`

  - .RDS: `base::saveRDS()`

  - .RData: `base::save(list=<symbol name>, envir=<temp env>)`
    preserving original symbol name

  - .csv: ``` utils::``write.csv() ``` (extra args via …)

## Object naming behaviour

`ucload()` naming for .qs2/.RDS/.csv:

- The assigned name is derived from the UC filename and converted to
  snake_case. For example `"IPAFFS to date with Rates UC test 20.qs2"`
  becomes `"ipaffs_to_date_with_rates_uc_test_20"`.

For .RData/.rda:

- All objects in the file are loaded with their original names (as
  stored in the file).

## Performance notes

- Prefer .qs2 for large R objects when you care about speed.
- Use `ucsave(..., extension = ".qs2")` for fast writes and `ucload()`
  for fast reads.
- For very large objects, `upload()` streams bytes and avoids loading
  the whole file into memory.

## Troubleshooting

- “file extension mismatch” warnings:
  - If `force_ext = TRUE`, destination extension is automatically
    adjusted.
  - If `force_ext = FALSE`, you may be prompted to continue or cancel.
- If an uploaded file cannot be loaded later:
  - Confirm you uploaded the correct format to the matching extension.
  - Confirm downloads are binary-safe (ucLoader `download()` is designed
    to preserve bytes).
  - If using your own download/upload code, ensure binary mode.

## License

MIT License.
