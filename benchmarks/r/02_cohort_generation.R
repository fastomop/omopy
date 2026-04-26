# Benchmark 02: Cohort Generation
# R equivalent: CDMConnector::generateConceptCohortSet()

source("benchmarks/r/00_helpers.R")
cat("=== 02: Cohort Generation ===\n")
t0 <- proc.time()

con <- dbConnect(duckdb(), dbdir = DB_PATH)
cdm <- cdmFromCon(con = con, cdmSchema = "main", writeSchema = "main", cdmName = "synthea_1k")

# Coronary arteriosclerosis (317576) — 1243 records in this DB
cdm <- generateConceptCohortSet(
  cdm = cdm,
  conceptSet = list(coronary_artery = 317576),
  name = "target_cohort"
)

counts <- cdm$target_cohort |>
  group_by(cohort_definition_id) |>
  summarise(n_records = n(), n_subjects = n_distinct(subject_id)) |>
  collect()

save_result(counts, "02_cohort_generation")
elapsed <- (proc.time() - t0)[["elapsed"]]
save_timing("02_cohort_generation", elapsed)
cdmDisconnect(cdm)
cat("Done in", elapsed, "seconds\n")