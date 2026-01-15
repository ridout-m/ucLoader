#' Validate Databricks connectivity (host + PAT)
#'
#' Prompts for `DATABRICKS_HOST` and `DATABRICKS_TOKEN` if missing, then
#' verifies access by calling the Unity Catalog catalogs endpoint.
#' Designed for RStudio Server sessions on Databricks.
#'
#' @param show_success Logical; if `TRUE`, shows a success dialog on connect.
#' @return Invisibly returns a list with `ok = TRUE` and `host`.
#' @details Requires a valid Databricks Personal Access Token (PAT).
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

#' Download a Unity Catalog file to a temp path
#'
#' Streams a file from `/Volumes/...` via the Files API to a local temporary
#' file and returns that path. Useful for passing into readers
#' such as `readr::read_csv()`, `readxl::read_excel()`, `base::load()`, etc.
#'
#' @param path Character scalar UC path that begins with `/Volumes/`.
#' @return Character scalar: local path of the downloaded file.
#' @examples
#' \dontrun{
#' p <- download("/Volumes/my_cat/my_schema/tables/foo.csv")
#' df <- read.csv(p)
#' }
#' @export
download <- function(path){

  connect(FALSE)

  seg   <- sub("^/", "", path)
  url   <- paste0(sub("/+$","", Sys.getenv("DATABRICKS_HOST")), "/api/2.0/fs/files/", utils::URLencode(seg, reserved = TRUE))

  dest  <- tempfile(fileext = ".RData")
  req   <- httr2::request(url) |>
    httr2::req_auth_bearer_token(Sys.getenv("DATABRICKS_TOKEN")) |>
    httr2::req_progress(type = "down")

  resp  <- httr2::req_perform(req, path = dest)

  return(dest)

}

#' List files in a Unity Catalog directory
#'
#' Lists objects in a UC volume directory and returns only files (not subdirs)
#' with basic metadata.
#'
#' @param dir Character scalar UC directory starting with `/Volumes/`.
#' @param pattern Optional regex to filter by file name.
#' @return A `data.frame` with columns:
#'   \describe{
#'     \item{path}{Full UC path}
#'     \item{name}{File name}
#'     \item{file_size_MB}{Size in MiB (bytes / 1024^2)}
#'     \item{last_modified}{POSIXct (UTC)}
#'   }
#' @examples
#' \dontrun{
#' UC_list_files("/Volumes/cat/schema/tables", pattern = "\\\\.RData$")
#' }
#' @export
UC_list_files <- function(dir, pattern = NULL) {

  connect(FALSE)

  seg <- sub("^/", "", dir)
  url <- paste0(sub("/+$", "", Sys.getenv("DATABRICKS_HOST")), "/api/2.0/fs/directories/", utils::URLencode(seg, reserved = TRUE))

  req <- httr2::request(url) |>
    httr2::req_auth_bearer_token(Sys.getenv("DATABRICKS_TOKEN"))

  resp <- httr2::req_perform(req)

  out <- httr2::resp_body_json(resp, simplifyVector = TRUE)

  df <- as.data.frame(out$contents, stringsAsFactors = FALSE, optional = TRUE)

  if(!is.null(pattern)){
    df <- df[
      !df$is_directory & grepl(pattern, df$name)
      ,
    ]
  } else {
    df <- df[
      !df$is_directory
      ,
    ]
  }

  df$last_modified <- as.POSIXct(df$last_modified / 1000, origin = "1970-01-01", tz = "UTC")
  df$file_size_MB  <- round(df$file_size / 1024^2, 2)

  return(df[c(1,5,6,4)])
}

#' Upload a local file into a Unity Catalog path
#'
#' Performs an HTTP PUT to write a local file into a UC Volumes destination.
#'
#' @param local_path Existing local file path to upload.
#' @param dest_uc_path Destination UC path beginning with `/Volumes/`.
#' @param overwrite Logical; if `TRUE`, allow overwrite of the destination.
#' @return Invisibly returns `dest_uc_path` on success.
#' @examples
#' \dontrun{
#' tmp <- tempfile(fileext = ".csv"); write.csv(mtcars, tmp, row.names = FALSE)
#' upload(tmp, "/Volumes/cat/schema/tables/mtcars.csv", overwrite = TRUE)
#' }
#' @export
upload <- function(local_path, dest_uc_path, overwrite = TRUE){

  connect(FALSE)

  seg <- sub("^/", "", dest_uc_path)
  url <- paste0(sub("/+$","", Sys.getenv("DATABRICKS_HOST")), "/api/2.0/fs/files/", utils::URLencode(seg, reserved = TRUE))

  req <- httr2::request(url) |>
    httr2::req_auth_bearer_token(Sys.getenv("DATABRICKS_TOKEN")) |>
    httr2::req_method("PUT") |>
    httr2::req_url_query(overwrite = if (overwrite) "true" else "false") |>
    httr2::req_body_file(local_path, type = "application/octet-stream") |>
    httr2::req_progress(type = "up")

  resp <- httr2::req_perform(req)

  invisible(dest_uc_path)
}

#' Load an `.RData` from Unity Catalog
#'
#' Downloads the UC file to a temporary location, loads objects into `envir`,
#' then deletes the temp file.
#'
#' @param path UC path to the `.RData`/`.rda` file (must start with `/Volumes/`).
#' @param envir Environment to load objects into (default parent frame).
#' @return (Invisibly) character vector of object names created by `base::load()`.
#' @examples
#' \dontrun{
#' UC_load("/Volumes/cat/schema/tables/snapshot.RData")
#' }
#' @export
UC_load <- function(path, envir = parent.frame()) {

  local_path <- download(path)

  on.exit(try(unlink(local_path), silent = TRUE), add = TRUE)

  cli::cli_inform("Loading...")
  objs <- base::load(local_path, envir = envir)

  cli::cli_alert_success("Loaded: {paste(objs, collapse = ', ')}")
  invisible(objs)
}

#' Read a CSV from Unity Catalog
#'
#' Downloads the UC file to a temp file and reads it with `utils::read.csv()`.
#'
#' @param path UC path to the `.csv` file (must start with `/Volumes/`).
#' @param ... Passed through to `utils::read.csv()`.
#' @param stringsAsFactors Logical; forwarded to `utils::read.csv()`.
#' @return A `data.frame`.
#' @examples
#' \dontrun{
#' df <- UC_read.csv("/Volumes/cat/schema/tables/foo.csv")
#' }
#' @export
UC_read.csv <- function(path, ..., stringsAsFactors = FALSE){

  local_path <- download(path)

  on.exit(try(unlink(local_path), silent = TRUE), add = TRUE)

  cli::cli_inform("Loading...")
  dat <- utils::read.csv(local_path, ..., stringsAsFactors = stringsAsFactors)

  fname <- basename(path)
  cli::cli_alert_success("Loaded {fname}")

  dat
}

#' Save R objects and upload as `.RData` to Unity Catalog
#'
#' Saves objects locally to a temporary `.RData` and uploads to a UC path.
#' Works like `base::save()`, but targets `/Volumes/...`.
#'
#' @param file Object to save (or a named object); see Details.
#' @param path Destination UC path, e.g. `/Volumes/cat/schema/tables/file.RData`.
#' @param overwrite Logical; if `TRUE`, allow overwrite at destination.
#' @details This wrapper currently saves the object provided in `file` using
#'   `base::save(file, file = tmp)`. If you need to save multiple named objects,
#'   adapt to collect their names with `substitute(list(...))` and call
#'   `do.call(save, list(list = obj_names, file = tmp))`.
#' @return Invisibly returns `path` on success.
#' @examples
#' \dontrun{
#' UC_save(mtcars, "/Volumes/cat/schema/tables/mtcars.RData", overwrite = TRUE)
#' }
#' @export
UC_save <- function(file, path, overwrite = TRUE) {

  tmp <- tempfile(fileext = ".RData")
  on.exit(try(unlink(tmp), silent = TRUE), add = TRUE)

  cli::cli_inform("Saving...")
  base::save(file, file = tmp)
  cli::cli_alert_success("Saved {file}")

  upload(local_path = tmp, dest_uc_path = path, overwrite = overwrite)
}


