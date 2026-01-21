#' Minimal Unity Catalog file helpers (REST)
#'
#' Convenience helpers to list, download, upload, and load files stored in
#' Databricks Unity Catalog (UC) Volumes via the Databricks REST API.
#'
#' These helpers use the Databricks Files API and require the environment
#' variables `DATABRICKS_HOST` and `DATABRICKS_TOKEN` (a PAT).
#'
#' @section Key functions:
#' \itemize{
#'   \item \code{\link{connect}}: Configure and validate host/token access (Unity Catalog connectivity check).
#'   \item \code{\link{list_files}}: List files under a UC Volume directory path.
#'   \item \code{\link{download}}: Download a UC file to a local temporary path (binary-safe).
#'   \item \code{\link{upload}}: Upload a local file to a UC path (binary-safe streaming; optional overwrite).
#'   \item \code{\link{ucload}}: Download then load/read into an environment (supports \code{.qs2}, \code{.RData}/\code{.rda}, \code{.RDS}, \code{.csv}).
#'   \item \code{\link{ucsave}}: Save an R object locally (supports \code{.qs2}, \code{.RData}, \code{.RDS}, \code{.csv}) then upload to UC
#'     (preserves object symbol name for \code{.RData}).
#' }
#'
#' @keywords internal
#' @name ucfs
"_PACKAGE"
NULL
