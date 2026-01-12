.uc_validate_pat <- function(host, token) {
  h <- curl::new_handle()
  curl::handle_setheaders(h, Authorization = paste("Bearer", token))
  url <- paste0(host, "/api/2.0/unity-catalog/catalogs?max_results=1")
  res <- try(curl::curl_fetch_memory(url, handle = h), silent = TRUE)
  if (inherits(res, "try-error")) return(list(ok = FALSE, code = NA, msg = conditionMessage(attr(res, "condition"))))
  list(ok = res$status_code == 200, code = res$status_code, msg = rawToChar(res$content))
}

.download_uc_file <- function(uc_path, dest = NULL, verbose = TRUE, retry_on_auth = TRUE) {

  host <- Sys.getenv("DATABRICKS_HOST", unset = "")
  if (!nzchar(host)){
    stop("DATABRICKS_HOST is not set. Ask your admin to set it in the cluster init script.")
  }

  token <- Sys.getenv("DATABRICKS_TOKEN", unset = "")
  if (!nzchar(token)){
    token <- getPass::getPass("Please enter your Databricks PAT: ")
    Sys.setenv(DATABRICKS_TOKEN = token)
  }

  check <- .uc_validate_pat(host, token)
  if (!isTRUE(check$ok) && retry_on_auth) {
    if (verbose) message("PAT appears invalid (status ", check$code, "). Please enter a valid PAT.")
    token <- getPass::getPass("PAT invalid/expired. Please enter a valid Databricks PAT: ")
    Sys.setenv(DATABRICKS_TOKEN = token)
    check <- .uc_validate_pat(host, token)
  }
  if (!isTRUE(check$ok)) stop("Token invalid (status ", check$code, ").")

  path_no_slash <- sub("^/", "", uc_path)
  url_a <- paste0(host, "/api/2.0/fs/files/",                              utils::URLencode(path_no_slash, reserved = TRUE))
  url_b <- paste0(host, "/api/2.1/unity-catalog/volumes/files/read?path=", utils::URLencode(uc_path,       reserved = TRUE))

  if (is.null(dest)) {
    ext <- tools::file_ext(uc_path); if (!nzchar(ext)) ext <- "bin"
    dest <- tempfile(fileext = paste0(".", ext))
  }

  h <- curl::new_handle()
  curl::handle_setheaders(h, Authorization = paste("Bearer", token))

  try_url <- function(u) {
    cli::cli_inform("Downloading...")
    tryCatch({
      curl::curl_download(u, destfile = dest, handle = h, quiet = !verbose)
      TRUE
    }, error = function(e) {
      if (verbose) message("  failed: ", conditionMessage(e))
      FALSE
    })
  }

  ok <- try_url(url_a) || try_url(url_b)
  if (!ok) stop("Internal error.")
  if (verbose && ok) cli::cli_alert_success("Downloaded to {dest}")
  dest
}

.uc_mkdirs <- function(host, token, uc_path, verbose = FALSE) {
  path_no_slash <- sub("^/", "", uc_path)
  url <- paste0(sub("/+$", "", host), "/api/2.0/fs/directories/", utils::URLencode(path_no_slash, reserved = TRUE))

  h <- curl::new_handle()
  curl::handle_setheaders(h, Authorization = paste("Bearer", token))
  curl::handle_setopt(h, customrequest = "PUT")

  tryCatch({
    curl::curl_fetch_memory(url, handle = h)
    TRUE
  }, error = function(e) {
    if (verbose) cat("  (mkdirs skipped: ", conditionMessage(e), ")\n", sep = "")
    FALSE
  })
}

.upload_uc_file <- function(local_path, uc_path, overwrite = TRUE, verbose = TRUE, retry_on_auth = TRUE) {

  host <- Sys.getenv("DATABRICKS_HOST", unset = "")
  if (!nzchar(host)){
    stop("DATABRICKS_HOST is not set. Ask your admin to set it in the cluster init script.")
  }

  token <- Sys.getenv("DATABRICKS_TOKEN", unset = "")
  if (!nzchar(token)) {
    token <- getPass::getPass("Please enter your Databricks PAT: ")
    Sys.setenv(DATABRICKS_TOKEN = token)
  }

  check <- .uc_validate_pat(host, token)
  if (!isTRUE(check$ok) && retry_on_auth) {
    if (verbose) message("PAT appears invalid (status ", check$code, "). Please enter a valid PAT.")
    token <- getPass::getPass("PAT invalid/expired. Please enter a valid Databricks PAT: ")
    Sys.setenv(DATABRICKS_TOKEN = token)
    check <- .uc_validate_pat(host, token)
  }
  if (!isTRUE(check$ok)) stop("Token invalid (status ", check$code, ").")

  stopifnot(file.exists(local_path), startsWith(uc_path, "/Volumes/"))

  parent <- sub("/[^/]+$", "", uc_path)
  .uc_mkdirs(host, token, parent, verbose = verbose)

  path_no_slash <- sub("^/", "", uc_path)
  q_over <- tolower(as.character(isTRUE(overwrite)))

  url_a <- paste0(host, "/api/2.0/fs/files/", utils::URLencode(path_no_slash, reserved = TRUE), "?overwrite=", q_over)
  url_b <- paste0(host, "/api/2.1/unity-catalog/volumes/files/write?path=", utils::URLencode(uc_path, reserved = TRUE), "&overwrite=", q_over)

  try_put <- function(u) {
    cli::cli_inform("Uploading...")
    hdr <- c(paste0("Authorization: Bearer ", token), "Content-Type: application/octet-stream")

    ok <- tryCatch({
      curl::curl_upload(file = local_path, url = u, httpheader = hdr, verbose = FALSE)
      TRUE
    }, error = function(e) {
      if (verbose) message("  (fallback) ", conditionMessage(e))
      FALSE
    })
    ok
  }


  ok <- try_put(url_a) || try_put(url_b)
  if (!ok) stop("Upload failed via both endpoints. Check path, permissions, or network.")
  if (verbose && ok) cli::cli_alert_success("Uploaded to {.path {uc_path}}")
  invisible(uc_path)
}


#' Load an .RData file from Unity Catalog
#'
#' Downloads the file to a temporary location, loads it into `envir`, then deletes the temp file.
#' Requires `DATABRICKS_HOST` to be set (e.g. via cluster init script).
#'
#' @param uc_path UC path to the .RData/.rda file.
#' @param envir Environment to load objects into.
#' @param ... Passed to internal downloader (e.g. `verbose`, `retry_on_auth`).
#' @return (Invisible) character vector of object names created by `load()`.
#' @export
UC_load <- function(uc_path, envir = parent.frame(), ...) {
  local_path <- .download_uc_file(uc_path, ...)
  on.exit(try(unlink(local_path), silent = TRUE), add = TRUE)

  cli::cli_inform("Loading...")
  objs <- base::load(local_path, envir = envir)

  cli::cli_alert_success("Loaded: {paste(objs, collapse = ', ')}")
  invisible(objs)
}

#' Read a CSV from Unity Catalog
#'
#' Downloads the file to a temporary location, reads it with `read.csv()`, then deletes the temp file.
#' Requires `DATABRICKS_HOST` to be set (e.g. via cluster init script).
#'
#' @param uc_path UC path to the .csv file.
#' @param ... Passed to `read.csv()`.
#' @param stringsAsFactors Passed to `read.csv()`.
#' @return A data.frame.
#' @export
UC_read.csv <- function(uc_path, ..., stringsAsFactors = FALSE){
  local_path <- .download_uc_file(uc_path, ...)
  on.exit(try(unlink(local_path), silent = TRUE), add = TRUE)

  cli::cli_inform("Loading...")
  dat <- utils::read.csv(local_path, ..., stringsAsFactors = stringsAsFactors)

  fname <- basename(uc_path)
  cli::cli_alert_success("Loaded {fname}")

  dat
}

#' Download a Unity Catalog Volume file to a local path
#'
#' Downloads any `/Volumes/...` file via the Unity Catalog Files API to a local file
#' (temporary by default) and returns that path. Handy for piping into readers such as
#' [readxl::read_excel()], [readr::read_csv()], [vroom::vroom()], etc.
#'
#' @param uc_path Character scalar. Must start with `/Volumes/`.
#' @param dest Optional local filesystem path. If `NULL`, a temp file is created.
#' @param ... Passed through to the internal downloader (e.g., `retry_on_auth = TRUE`).
#'
#' @return Character scalar: the local file path that was written.
#' @examples
#' \dontrun{
#' p <- uc_download("/Volumes/prd_dash_lab/aphw_restricted/tables/.../file.xlsx")
#' df <- readxl::read_excel(p)
#' }
#' @export
UC_download <- function(uc_path, dest = NULL, ...) {
  local_path <- .download_uc_file(uc_path, dest = dest, ...)
  cli::cli_inform("Downloaded {.file {basename(uc_path)}} to {.path {local_path}}")
  invisible(local_path)
}

#' Save R objects into a Unity Catalog Volume (.RData) and upload
#'
#' Works like [base::save()], but writes to a UC Volume path. Objects are saved
#' locally to a temporary `.RData` file and then uploaded to `uc_path` using
#' the Unity Catalog Files API.
#'
#' @param uc_path Character scalar UC path starting with `/Volumes/.../file.RData`.
#' @param ... Objects to save (as in [base::save]).
#' @param overwrite Logical; if `TRUE`, allow overwrite on the UC destination.
#' @param compress Passed to [base::save()] (default `TRUE`).
#' @param version Passed to [base::save()] (e.g., 2).
#' @param verbose Logical; progress messages.
#'
#' @return Invisibly returns `uc_path` on success.
#' @export
UC_save <- function(uc_path, ..., overwrite = TRUE, compress = TRUE, version = NULL, verbose = TRUE) {
  stopifnot(is.character(uc_path), length(uc_path) == 1L, startsWith(uc_path, "/Volumes/"))
  if (!grepl("\\.RData$|\\.rda$", uc_path, ignore.case = TRUE)) {
    uc_path <- paste0(uc_path, ".RData")
  }

  tmp <- tempfile(fileext = ".RData")
  on.exit(try(unlink(tmp), silent = TRUE), add = TRUE)
  cli::cli_inform("Saving...")

  obj_names <- as.character(substitute(list(...)))[-1L]
  args <- list(list = obj_names, file = tmp, compress = compress)
  if (!is.null(version)) args$version <- version
  do.call(base::save, args)

  .upload_uc_file(local_path = tmp, uc_path = uc_path, overwrite = overwrite, verbose = verbose)
}


