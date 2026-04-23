# Shared helpers for R benchmark scripts
# Sources this file at the top of each script.

library(CDMConnector)
library(DBI)
library(duckdb)
library(dplyr, warn.conflicts = FALSE)
library(readr)

RSCRIPT <- "C:\\Users\\Ian.Farr\\AppData\\Local\\Programs\\R\\R-4.5.2\\bin\\Rscript.exe"
DB_PATH <- file.path("data", "synthea_1k.duckdb")
RESULTS_DIR <- file.path("benchmarks", "r", "results")

dir.create(RESULTS_DIR, showWarnings = FALSE, recursive = TRUE)

connect_cdm <- function() {
  con <- dbConnect(duckdb(), dbdir = DB_PATH, read_only = TRUE)
  cdm <- cdmFromCon(
    con = con,
    cdmSchema = "main",
    writeSchema = "main",
    cdmName = "synthea_1k"
  )
  return(list(con = con, cdm = cdm))
}

disconnect_cdm <- function(conn) {
  cdmDisconnect(conn$cdm)
}

save_result <- function(df, name) {
  path <- file.path(RESULTS_DIR, paste0(name, ".csv"))
  write_csv(as.data.frame(df), path)
  cat("Saved:", path, "(", nrow(df), "rows )\n")
}

save_timing <- function(name, elapsed_seconds) {
  path <- file.path(RESULTS_DIR, paste0(name, "_timing.csv"))
  write_csv(data.frame(benchmark = name, elapsed_s = elapsed_seconds, language = "R"), path)
}