default_obj_name <- function(path, fallback = "default_data") {
  nm <- tools::file_path_sans_ext(basename(path))
  nm <- tolower(nm)
  nm <- gsub(" ", "_", nm)
  if (!nzchar(nm)) nm <- fallback
  if (grepl("^[0-9]", nm)) nm <- paste0("_", nm)
  nm
}

do_assign <- function(obj, nm, envir) {
  assign(nm, obj, envir)
  cli::cli_alert_info(
    "Data outputted as {.val {nm}}. Note: only {.val .RData/.rda} supports unassigned loading by default."
  )
  invisible(nm)
}


#' Connect to Databricks by validating host + PAT via Unity Catalog
#'
#' Ensures `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are available (prompting in
#' RStudio if missing), then checks connectivity/authorisation by calling the
#' Unity Catalog catalogs endpoint (`/api/2.0/unity-catalog/catalogs`).
#' On failure, shows an interactive dialog with the HTTP/transport error and
#' optionally allows retrying after re-entering credentials.
#'
#' @param show_success Logical; if `TRUE`, shows a "Success" dialog when a
#'   connection check returns HTTP 200.
#'
#' @return Invisibly returns a list with elements:
#'   \describe{
#'     \item{ok}{Logical, always `TRUE` on success.}
#'     \item{host}{Character; the value of `DATABRICKS_HOST`.}
#'   }
#'
#' @details
#' This function is designed for interactive RStudio sessions and uses
#' `rstudioapi` to prompt for the host and PAT (stored in environment variables).
#' The request is made with `curl` using a Bearer token header. Non-200 status
#' codes trigger a failure dialog; the user can choose to retry or cancel.
#'
#' @examples
#' \dontrun{
#' # Interactive: prompts for missing env vars and validates access.
#' connect()
#'
#' # Same, but without a success dialog:
#' connect(show_success = FALSE)
#' }
#'
#' @export
connect <- function(show_success = TRUE) {

  if (Sys.getenv("DATABRICKS_HOST") == "") {
    Sys.setenv(
      DATABRICKS_HOST = rstudioapi::showPrompt("DATABRICKS_HOST", "Please enter Databricks Host: ")
    )
  }
  if (Sys.getenv("DATABRICKS_TOKEN") == "") {
    Sys.setenv(
      DATABRICKS_TOKEN = rstudioapi::askForSecret(
        "DATABRICKS_TOKEN",
        "Please enter your Personal Access Token (PAT): ",
        "DATABRICKS_TOKEN"
      )
    )
  }

  repeat {

    h <- curl::new_handle()
    curl::handle_setheaders(h, Authorization = paste("Bearer", Sys.getenv("DATABRICKS_TOKEN")))
    url <- paste0(Sys.getenv("DATABRICKS_HOST"), "/api/2.0/unity-catalog/catalogs?max_results=1")
    res <- try(curl::curl_fetch_memory(url, handle = h), silent = TRUE)

    if (inherits(res, "try-error")) {

      err <- conditionMessage(attr(res, "condition"))

      rstudioapi::showDialog(
        "Failure",
        paste0(
          "<p>Unable to connect to host: </p>",
          "<p><b>", Sys.getenv("DATABRICKS_HOST"), "</b></p>",
          "<p></p>",
          "<p>HTTP failure: ", err, "</p>"
        )
      )
    } else if (!identical(res$status_code, 200L)) {

      codes <- c(
        "Okay." = '200',
        "Invalid token." = '401',
        "Token valid but not authorised for that endpoint/workspace." = '403',
        "Wrong host/workspace, wrong path, endpoint not enabled." = '404'
      )

      code <- res$status_code

      message <- names(which(codes == code))

      rstudioapi::showDialog(
        "Failure",
        paste0(
          "<p>Unable to connect to host: </p>",
          "<p><b>", Sys.getenv("DATABRICKS_HOST"), "</b></p>",
          "<p>Transport failure: ", message, "</p>"
        )
      )
    } else if (identical(res$status_code, 200L)){

      if (show_success){
        rstudioapi::showDialog(
          "Success",
          paste0(
            "<p>Connected to host:</p>",
            "<p><b>", Sys.getenv("DATABRICKS_HOST"), "</b></p>"
          )
        )
      }
      return(invisible(list(ok = TRUE, host = Sys.getenv("DATABRICKS_HOST"))))
    }

    tryagain <- rstudioapi::showQuestion(
      "Try again?",
      "The last attempt failed. Review credentials and retry?",
      "Yes", "No"
    )
    if (!tryagain) stop("User cancelled.")
    Sys.setenv(
      DATABRICKS_HOST  = rstudioapi::showPrompt("DATABRICKS_HOST", "Please enter Databricks Host: "),
      DATABRICKS_TOKEN = rstudioapi::askForSecret(
        "DATABRICKS_TOKEN",
        "Please enter your Personal Access Token (PAT): ",
        "DATABRICKS_TOKEN"
      )
    )
  }
}

#' Download a Unity Catalog file to a local temporary path
#'
#' Downloads a single file from a Unity Catalog volume path (starting with
#' `/Volumes/`) using the Databricks Files API (`/api/2.0/fs/files/...`) and
#' saves it to a local temporary file. Intended for workflows where downstream
#' functions require a local filesystem path (e.g., `read.csv()`, `readr::read_csv()`,
#' `readxl::read_excel()`, `base::load()`).
#'
#' @param path Character scalar. A Unity Catalog file path beginning with
#'   `/Volumes/`.
#'
#' @return Character scalar. The local temporary file path containing the
#'   downloaded contents.
#'
#' @details
#' Calls `connect(FALSE)` to ensure `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
#' are available. The host is normalised to remove trailing slashes, and the
#' UC path is URL-encoded before constructing the Files API URL.
#'
#' The destination filename is created with `tempfile()`. The file extension is
#' inferred from `path`; if none is present, `.bin` is used. Progress is reported
#' via `cli`. Download errors are converted to `cli::cli_abort()`.
#'
#' @examples
#' \dontrun{
#' # Download to a temp file, then read it locally
#' p <- download("/Volumes/catalog/schema/volume/path/to/foo.csv")
#' df <- read.csv(p)
#'
#' # Example for Excel
#' x <- download("/Volumes/catalog/schema/volume/path/to/data.xlsx")
#' tab <- readxl::read_excel(x)
#' }
#'
#' @export
download <- function(path){

  connect(FALSE)

  host  <- sub("/+$","", Sys.getenv("DATABRICKS_HOST"))
  token <- Sys.getenv("DATABRICKS_TOKEN")

  seg    <- sub("^/", "", path)
  url2.0 <- paste0(host, "/api/2.0/fs/files/", utils::URLencode(seg, reserved = TRUE))

  ext   <- tools::file_ext(path); if (!nzchar(ext)) ext <- "bin"
  dest  <- tempfile(fileext = paste0(".", ext))

  h <- curl::new_handle()
  curl::handle_setheaders(h, Authorization = paste("Bearer", token))

  file <- sub(".*/", "", path)

  cli::cli_progress_step("Downloading {.file {file}} to {.path {dest}}")

  try <- tryCatch({
    curl::curl_download(url = url2.0, destfile = dest, handle = h, quiet = TRUE)
  }, error = function(e){
    cli::cli_progress_done(result = "failed")
    cli::cli_abort("{conditionMessage(e)}")
  })

  cli::cli_progress_done()

  return(dest)

}

#' List files in a Unity Catalog directory
#'
#' Lists the contents of a Unity Catalog volume directory (a path beginning with
#' `/Volumes/`) using the Databricks Files API directories endpoint and returns
#' a `data.frame` of files with basic metadata. If `pattern` is supplied, results
#' are filtered by filename using regular expressions.
#'
#' @param dir Character scalar. A Unity Catalog directory path beginning with
#'   `/Volumes/`.
#' @param pattern Optional character scalar. A regular expression used to filter
#'   results by `name`.
#' @param ignore_case Logical; passed to `grepl(ignore.case = ...)` when
#'   `pattern` is provided.
#'
#' @return A `data.frame` with columns:
#'   \describe{
#'     \item{name}{File name.}
#'     \item{path}{Full UC path.}
#'     \item{file_size_MB}{File size in MiB, rounded to 2 decimals.}
#'     \item{last_modified}{POSIXct timestamp in UTC.}
#'   }
#' If the directory exists but has no `contents`, returns `NULL` invisibly.
#'
#' @details
#' Calls `connect(FALSE)` to ensure `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are
#' available. The directory path is URL-encoded and requested as JSON. The API
#' returns `last_modified` as milliseconds since epoch; this is converted to a
#' UTC `POSIXct`. File sizes are converted from bytes to MiB.
#'
#' Progress and errors are reported via `cli`. Transport failures trigger
#' `cli::cli_abort()`.
#'
#' @examples
#' \dontrun{
#' # List everything
#' list_files("/Volumes/cat/schema/volume/path")
#'
#' # Filter by extension (regex) and ignore case
#' list_files("/Volumes/cat/schema/volume/path", pattern = "\\\\.rds$", ignore_case = TRUE)
#' }
#'
#' @export
list_files <- function(dir, pattern = NULL, ignore_case = FALSE) {

  connect(FALSE)

  host  <- sub("/+$","", Sys.getenv("DATABRICKS_HOST"))
  token <- Sys.getenv("DATABRICKS_TOKEN")

  seg    <- sub("^/", "", dir)
  url2.0 <- paste0(host, "/api/2.0/fs/directories/", utils::URLencode(seg, reserved = TRUE))

  h <- curl::new_handle()
  curl::handle_setheaders(h, Authorization = paste("Bearer", token), Accept = "application/json")
  curl::handle_setopt(h, customrequest = "GET")

  ptn <- if(is.null(pattern)) "all" else pattern

  cli::cli_progress_step("Listing {ptn} files in {.path {dir}}\n")

  resp <- tryCatch(
    curl::curl_fetch_memory(url = url2.0, handle = h),
    error = function(e) {
      cli::cli_progress_done(result = "failed")
      cli::cli_abort("{conditionMessage(e)}\n")
    }
  )

  out  <- jsonlite::fromJSON(rawToChar(resp$content), simplifyVector = TRUE)

  if (is.null(out$contents)){
    cli::cli_progress_done()
    cli::cli_alert_warning("Warning: No such list found\n")
    invisible(return(NULL))
  }

  df <- as.data.frame(out$contents, stringsAsFactors = FALSE, optional = TRUE)

  if (!is.null(pattern)){
    alt <- paste(sprintf("(%s)", pattern), collapse = "|")
    df  <- df[grepl(alt, df$name, ignore.case = ignore_case), ]
  }

  df$last_modified <- as.POSIXct(df$last_modified / 1000, origin = "1970-01-01", tz = "UTC")
  df$file_size_MB  <- round(df$file_size / 1024^2, 2)

  cli::cli_progress_done()

  df[c("name", "path", "file_size_MB", "last_modified")]

}

#' Upload a local file to a Unity Catalog volume path
#'
#' Uploads a local file to a Unity Catalog destination (a path beginning with
#' `/Volumes/`) using the Databricks Files API (`/api/2.0/fs/files/...`) with an
#' HTTP upload and optional overwrite. Can also enforce that the destination file
#' extension matches the local file extension.
#'
#' @param object Character scalar. Existing local file path to upload.
#' @param path Character scalar. Destination Unity Catalog file path beginning
#'   with `/Volumes/`.
#' @param overwrite Logical; if `TRUE`, uploads with `overwrite=true` so an
#'   existing destination file may be replaced.
#' @param force_ext Logical; if `TRUE`, replaces the extension of `path` with the
#'   extension of `object`. If `FALSE` and extensions differ, the user is warned
#'   and prompted whether to continue.
#'
#' @return Invisibly returns the final destination UC path (after any extension
#'   adjustment). If the user cancels after an extension mismatch warning,
#'   returns `FALSE` invisibly.
#'
#' @details
#' Calls `connect(FALSE)` to ensure `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are
#' available. The destination path is URL-encoded and uploaded as
#' `application/octet-stream`. Progress and errors are reported via `cli`;
#' transport failures trigger `cli::cli_abort()`.
#'
#' @examples
#' \dontrun{
#' # Save an object locally then upload to UC
#' x <- data.frame(
#'   int  = sample(1e3, replace = TRUE),
#'   num  = rnorm(1e3),
#'   char = sample(state.name, 1e3, replace = TRUE),
#'   stringsAsFactors = FALSE
#' )
#' tmp_file <- tempfile(fileext = ".qs2")
#' qs2::qs_save(x, tmp_file)
#'
#' # Upload (extension of 'path' will be forced to .qs2 if force_ext = TRUE)
#' upload(tmp_file, "/Volumes/cat/schema/volume/path/saved temp file.qs2")
#' }
#'
#' @export
upload <- function(object, path, overwrite = TRUE, force_ext = TRUE) {

  connect(FALSE)

  host  <- sub("/+$","", Sys.getenv("DATABRICKS_HOST"))
  token <- Sys.getenv("DATABRICKS_TOKEN")

  q_over <- tolower(as.character(isTRUE(overwrite)))

  ext_file <- tools::file_ext(object)
  ext_path <- tools::file_ext(path)

  if (force_ext){
    path <- paste0(tools::file_path_sans_ext(path), ".", ext_file)
  } else if (!identical(tools::file_ext(object), ext_path)){
    cli::cli_alert_warning("Warning: The file extension {.val {ext_file}} does not match the path extension {.val {ext_path}}.")
    okay <- utils::askYesNo("Would you like to continue? The resulting output file may not be readable by the expected loader.", default = FALSE)
    if (!isTRUE(okay)){
      cli::cli_alert_warning("Operation Cancelled")
      return(invisible(FALSE))
    }
  }

  seg <- sub("^/", "", path)
  url2.0 <- paste0(host, "/api/2.0/fs/files/", utils::URLencode(seg, reserved = TRUE), "?overwrite=", q_over)

  cli::cli_progress_step("Uploading {.file {object}} to {.path {path}}")

  hdr <- c(paste0("Authorization: Bearer ", token), "Content-Type: application/octet-stream")

  try <- tryCatch({
    curl::curl_upload(file = object, url = url2.0, httpheader = hdr, verbose = FALSE)
  }, error = function(e) {
    cli::cli_progress_done(result = "failed")
    cli::cli_abort("{conditionMessage(e)}")
  })

  cli::cli_progress_done()

  invisible(path)
}

#' Load a Unity Catalog file into an environment
#'
#' Downloads a Unity Catalog file to a temporary local path, reads it based on
#' its extension, assigns the resulting object(s) into `envir`, and deletes the
#' temporary file on exit.
#'
#' Supported extensions are `.qs2`, `.RData`/`.rda`, `.RDS`, and `.csv`.
#'
#' @param path Character scalar. Unity Catalog file path (typically under
#'   `/Volumes/...`).
#' @param ... Additional arguments passed to `utils::read.csv()` when `path` has
#'   extension `.csv`.
#' @param envir Environment into which objects are loaded/assigned. Defaults to
#'   `parent.frame()`.
#'
#' @return Invisibly returns:
#' \describe{
#'   \item{For `.RData`/`.rda`:}{A character vector of object names created by
#'   `base::load()`.}
#'   \item{For `.qs2`, `.RDS`, `.csv`:}{The assigned object name (character
#'   scalar).}
#' }
#'
#' @details
#' The file is first downloaded via `download()`. A temporary local file is
#' removed via `on.exit()`. For `.qs2`, `.RDS`, and `.csv`, the object is read
#' and assigned into `envir` using a name derived from the filename via
#' `default_obj_name()`. For `.RData`/`.rda`, objects are loaded via `base::load()`.
#'
#' Progress and user-facing messages are emitted via `cli`. If the extension is
#' unsupported, the function aborts.
#'
#' @examples
#' \dontrun{
#' # Load an .RData into the current environment
#' ucload("/Volumes/cat/schema/volume/path/snapshot.RData")
#'
#' # Load a .qs2 and assign into a specific environment
#' e <- new.env(parent = emptyenv())
#' nm <- ucload("/Volumes/cat/schema/volume/path/model.qs2", envir = e)
#' ls(e)
#' }
#'
#' @export
ucload <- function(path, ..., envir = parent.frame()) {

  local_path <- download(path)

  on.exit(try(unlink(local_path), silent = TRUE), add = TRUE)

  file <- sub(".*/", "", path)

  cli::cli_progress_step("Loading {.file {file}}")

  ext_path <- tolower(tools::file_ext(path))

  out <- switch(
    ext_path,
    "rdata" = {
      cli::cli_alert_info("Tip: Convert this file to {.val .qs2} for faster read/write.")
      base::load(local_path, envir = envir)
    },
    "rda" = {
      cli::cli_alert_info("Tip: Convert this file to {.val .qs2} for faster read/write.")
      base::load(local_path, envir = parent.frame())
    },
    "rds" = {
      obj <- base::readRDS(local_path)
      nm  <- default_obj_name(file)
      do_assign(obj, nm, envir)
    },
    "qs2" = {
      obj <- qs2::qs_read(local_path)
      nm  <- default_obj_name(file)
      do_assign(obj, nm, envir)
    },
    "csv" = {
      obj <- utils::read.csv(local_path, ...)
      nm  <- default_obj_name(file)
      do_assign(obj, nm, envir)
    },
    cli::cli_abort("\nFile path extension not supported by ucload()")
  )

  cli::cli_progress_done()
  invisible(out)
}

#' Save an R object to a temporary file and upload to Unity Catalog
#'
#' Serialises an R object to a temporary local file (chosen by `extension`) and
#' uploads that file to a Unity Catalog destination path via `upload()`.
#'
#' Supported output formats are `.qs2`, `.RData`, `.RDS`, and `.csv`.
#'
#' @param object R object to save.
#' @param path Character scalar. Destination Unity Catalog file path (typically
#'   beginning with `/Volumes/...`).
#' @param overwrite Logical; if `TRUE`, allow overwriting an existing file at the
#'   destination.
#' @param extension Character scalar. One of `c(".qs2", ".RData", ".RDS", ".csv")`
#'   indicating the temporary on-disk format to write before uploading.
#' @param force_ext Logical; passed to `upload()`. If `TRUE`, forces the
#'   destination filename extension to match the extension of the temporary file.
#' @param ... Additional arguments passed to `utils::write.csv()` when
#'   `extension = ".csv"`.
#'
#' @return Invisibly returns the destination UC path (character scalar) on
#'   success (returned from `upload()`).
#'
#' @details
#' The object is written to a temporary file created with `tempfile()`, which is
#' deleted on exit. For `.RData`, the object is saved under its calling name
#' (derived via `deparse(substitute(object))`) by assigning it into a temporary
#' environment and calling `base::save(list = <name>, envir = <env>)`. For `.RDS`
#' and `.qs2`, the object is written with `saveRDS()` or `qs2::qs_save()`,
#' respectively. For `.csv`, the object is written with `utils::write.csv()`.
#'
#' Progress, file size, and compression hints are reported via `cli`.
#'
#' @examples
#' \dontrun{
#' # Default: save as .qs2 then upload
#' ucsave(mtcars, "/Volumes/cat/schema/volume/path/mtcars.qs2", overwrite = TRUE)
#'
#' # Save as .RData (stores under the calling name)
#' x <- mtcars
#' ucsave(x, "/Volumes/cat/schema/volume/path/x", extension = ".RData")
#'
#' # Save as .csv with write.csv options
#' ucsave(mtcars, "/Volumes/cat/schema/volume/path/mtcars.csv",
#'        extension = ".csv", row.names = FALSE)
#' }
#'
#' @export
ucsave <- function(object, path, overwrite = TRUE, extension = c(".qs2", ".RData", ".RDS", ".csv"), force_ext = TRUE, ...) {

  extension <- match.arg(extension); tmp <- tempfile(fileext = extension)

  on.exit(try(unlink(tmp), silent = TRUE), add = TRUE)

  obj_label <- deparse(substitute(object))
  cli::cli_progress_step("Saving {.file {obj_label}} to {.path {tmp}}")

  if (extension == ".RData"){
    cli::cli_alert_info("Tip: Convert this file to {.val .qs2} for faster read/write.")
    e <- new.env(parent = emptyenv())
    assign(obj_label, object, envir = e)
    base::save(list = obj_label, file = tmp, envir = e, compress = TRUE)

  } else if (extension == ".RDS"){
    base::saveRDS(object, file = tmp)

  } else if (extension == ".qs2"){
    qs2::qs_save(object, file = tmp)

  } else if (extension == ".csv"){
    utils::write.csv(object, file = tmp, ..., )

  } else cli::cli_abort("\nFile path extension not supported by ucsave()")

  cli::cli_progress_done()

  if (extension != ".csv") comp <- "(Compressed)" else comp <- "(Uncompressed)"

  size <- round(file.size(tmp) / 1024 ^ 2, 2)
  cli::cli_alert_info("File for upload is {size}MB {comp}")

  upload(object = tmp, path = path, overwrite = overwrite, force_ext = force_ext)

}


