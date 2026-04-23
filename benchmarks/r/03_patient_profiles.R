# Benchmark 03: Patient Profiles
# R equivalent: PatientProfiles::addDemographics()

source("benchmarks/r/00_helpers.R")
library(PatientProfiles)
cat("=== 03: Patient Profiles ===\n")
t0 <- proc.time()

con <- dbConnect(duckdb(), dbdir = DB_PATH)
cdm <- cdmFromCon(con = con, cdmSchema = "main", writeSchema = "main", cdmName = "synthea_1k")

cdm <- generateConceptCohortSet(
  cdm = cdm,
  conceptSet = list(coronary_artery = 317576),
  name = "target_cohort"
)

enriched <- cdm$target_cohort |>
  addDemographics() |>
  collect()

# Save first 100 rows
save_result(head(enriched, 100), "03_patient_profiles")
elapsed <- (proc.time() - t0)[["elapsed"]]
save_timing("03_patient_profiles", elapsed)
cdmDisconnect(cdm)
cat("Done in", elapsed, "seconds\n")