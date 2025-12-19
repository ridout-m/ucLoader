#' ucLoader: Helpers for Databricks Unity Catalog file loading
#'
#' @description
#' `ucLoader` provides convenience functions to download files from Databricks
#' Unity Catalog into a temporary file and then load them into R.
#'
#' The Databricks workspace URL must be provided via the environment variable
#' `DATABRICKS_HOST`. Authentication uses a per-session PAT stored in
#' `DATABRICKS_TOKEN`, and is prompted securely via `{getPass}` when missing.
#'
#' @section Main functions:
#' - \link{UC_load} Load an `.RData`/`.rda` file into an environment.
#' - \link{UC_read.csv} Read a CSV into a data.frame.
#'
#' @section Required environment variables:
#' - `DATABRICKS_HOST` (set by cluster init script)
#' - `DATABRICKS_TOKEN` (set interactively)
#'
"_PACKAGE"

## usethis namespace: start
## usethis namespace: end
NULL
