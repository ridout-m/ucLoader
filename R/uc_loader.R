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
