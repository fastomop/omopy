# Benchmark 04: Cohort Characteristics
# R equivalent: CohortCharacteristics::summariseCharacteristics()

source("benchmarks/r/00_helpers.R")
library(CohortCharacteristics)
cat("=== 04: Cohort Characteristics ===\n")
t0 <- proc.time()

con <- dbConnect(duckdb(), dbdir = DB_PATH)
cdm <- cdmFromCon(con = con, cdmSchema = "main", writeSchema = "main", cdmName = "synthea_1k")

cdm <- generateConceptCohortSet(
  cdm = cdm,
  conceptSet = list(coronary_artery = 317576),
  name = "target_cohort"
)

result <- summariseCharacteristics(cdm$target_cohort)
save_result(as.data.frame(result), "04_characteristics")
elapsed <- (proc.time() - t0)[["elapsed"]]
save_timing("04_characteristics", elapsed)
cdmDisconnect(cdm)
cat("Done in", elapsed, "seconds\n")