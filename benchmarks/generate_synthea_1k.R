# Generate a Synthea-based OMOP CDM DuckDB for benchmarking
# Uses CDMConnector's Eunomia infrastructure to download a pre-built dataset
# (Parquet format), then loads it into a DuckDB file.
#
# Usage:
#   Rscript benchmarks/generate_synthea_1k.R
#
# Output:
#   data/synthea_1k.duckdb  (10K-patient Synthea dataset in OMOP CDM v5.3)

library(CDMConnector)
library(DBI)
library(duckdb)

# --- Configuration ---
dataset_name <- "synthea-medications-10k"
output_path  <- file.path("data", "synthea_1k.duckdb")
cache_dir    <- file.path("benchmarks", ".eunomia_cache")

# Create cache directory
dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
Sys.setenv(EUNOMIA_DATA_FOLDER = normalizePath(cache_dir))

cat("Downloading Eunomia dataset:", dataset_name, "\n")

# Download the dataset
downloadEunomiaData(datasetName = dataset_name, pathToData = cache_dir)

# Find the zip and extract
zip_file <- file.path(cache_dir, paste0(dataset_name, "_5.3.zip"))
extract_dir <- file.path(cache_dir, dataset_name)

if (!file.exists(zip_file)) {
  stop("Zip file not found: ", zip_file)
}

cat("Extracting...\n")
dir.create(extract_dir, showWarnings = FALSE, recursive = TRUE)
unzip(zip_file, exdir = extract_dir, overwrite = TRUE)

# Find parquet files
parquet_dir <- file.path(extract_dir, dataset_name)
if (!dir.exists(parquet_dir)) {
  # Maybe files are directly in extract_dir
  parquet_dir <- extract_dir
}

parquet_files <- list.files(parquet_dir, pattern = "\\.parquet$", full.names = TRUE)
cat("Found", length(parquet_files), "parquet files\n")

if (length(parquet_files) == 0) {
  stop("No parquet files found")
}

# Remove existing output
if (file.exists(output_path)) {
  file.remove(output_path)
}
wal <- paste0(output_path, ".wal")
if (file.exists(wal)) file.remove(wal)

# Create DuckDB and load parquet files as tables in 'main' schema
con <- dbConnect(duckdb(), dbdir = output_path)

for (pf in parquet_files) {
  table_name <- tools::file_path_sans_ext(basename(pf))
  cat("  Loading", table_name, "...")
  sql <- sprintf("CREATE TABLE main.%s AS SELECT * FROM read_parquet('%s')",
                 table_name, gsub("\\\\", "/", pf))
  dbExecute(con, sql)
  n <- dbGetQuery(con, sprintf("SELECT count(*) as n FROM main.%s", table_name))$n
  cat(" ", n, "rows\n")
}

# Verify
person_count <- dbGetQuery(con, "SELECT count(*) as n FROM main.person")$n
cat("\nPerson count:", person_count, "\n")

tables <- dbGetQuery(con, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
cat("Tables (", nrow(tables), "):", paste(sort(tables$table_name), collapse = ", "), "\n")

dbDisconnect(con, shutdown = TRUE)

cat("\nDone! Dataset ready at:", output_path, "\n")
cat("File size:", round(file.info(output_path)$size / 1024 / 1024, 1), "MB\n")