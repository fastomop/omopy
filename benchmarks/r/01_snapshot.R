# Benchmark 01: CDM Snapshot
# R equivalent: CDMConnector::snapshot()

source("benchmarks/r/00_helpers.R")

cat("=== 01: CDM Snapshot ===\n")

t0 <- proc.time()

con <- dbConnect(duckdb(), dbdir = DB_PATH)
cdm <- cdmFromCon(
  con = con,
  cdmSchema = "main",
  writeSchema = "main",
  cdmName = "synthea_1k"
)

snap <- snapshot(cdm)
save_result(snap, "01_snapshot")

elapsed <- (proc.time() - t0)[["elapsed"]]
save_timing("01_snapshot", elapsed)

cdmDisconnect(cdm)
cat("Done in", elapsed, "seconds\n")