# Benchmark 10: Drug Exposure Diagnostics
# R equivalent: DrugExposureDiagnostics::executeChecks()

source("benchmarks/r/00_helpers.R")
library(DrugExposureDiagnostics)
cat("=== 10: Drug Diagnostics ===\n")
t0 <- proc.time()

con <- dbConnect(duckdb(), dbdir = DB_PATH)
cdm <- cdmFromCon(con = con, cdmSchema = "main", writeSchema = "main", cdmName = "synthea_1k")

# Run diagnostics on clopidogrel (1322184)
result <- executeChecks(
  cdm = cdm,
  ingredients = 1322184,
  checks = c("missing", "exposureDuration", "type", "route", "quantity")
)

# Save summary info about each check
summary_rows <- data.frame(
  check_name = character(),
  n_rows = integer(),
  stringsAsFactors = FALSE
)
for (nm in names(result)) {
  df <- result[[nm]]
  if (!is.null(df) && is.data.frame(df)) {
    summary_rows <- rbind(summary_rows, data.frame(
      check_name = nm,
      n_rows = nrow(df),
      stringsAsFactors = FALSE
    ))
  }
}

save_result(summary_rows, "10_drug_diagnostics")
elapsed <- (proc.time() - t0)[["elapsed"]]
save_timing("10_drug_diagnostics", elapsed)
cdmDisconnect(cdm)
cat("Done in", elapsed, "seconds\n")