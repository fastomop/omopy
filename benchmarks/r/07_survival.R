# Benchmark 07: Cohort Survival
# R equivalent: CohortSurvival::estimateSingleEventSurvival()

source("benchmarks/r/00_helpers.R")
library(CohortSurvival)
cat("=== 07: Survival ===\n")
t0 <- proc.time()

con <- dbConnect(duckdb(), dbdir = DB_PATH)
cdm <- cdmFromCon(con = con, cdmSchema = "main", writeSchema = "main", cdmName = "synthea_1k")

# Target: coronary arteriosclerosis; Outcome: myocardial infarction
cdm <- generateConceptCohortSet(
  cdm = cdm,
  conceptSet = list(coronary_artery = 317576, mi = 4329847),
  name = "survival_cohorts"
)

result <- estimateSingleEventSurvival(
  cdm = cdm,
  targetCohortTable = "survival_cohorts",
  targetCohortId = 1,
  outcomeCohortTable = "survival_cohorts",
  outcomeCohortId = 2
)

save_result(as.data.frame(result), "07_survival")
elapsed <- (proc.time() - t0)[["elapsed"]]
save_timing("07_survival", elapsed)
cdmDisconnect(cdm)
cat("Done in", elapsed, "seconds\n")