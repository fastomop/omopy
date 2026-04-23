# Benchmark 09: Treatment Patterns
# R equivalent: TreatmentPatterns::computePathways()

source("benchmarks/r/00_helpers.R")
library(TreatmentPatterns)
cat("=== 09: Treatment Patterns ===\n")
t0 <- proc.time()

con <- dbConnect(duckdb(), dbdir = DB_PATH)
cdm <- cdmFromCon(con = con, cdmSchema = "main", writeSchema = "main", cdmName = "synthea_1k")

# Target: coronary arteriosclerosis; Events: clopidogrel + simvastatin
cdm <- generateConceptCohortSet(
  cdm = cdm,
  conceptSet = list(
    coronary_artery = 317576,
    clopidogrel = 1322184,
    simvastatin = 1539403
  ),
  name = "tp_cohorts"
)

# Check what cohort IDs were generated
cohort_set <- omopgenerics::settings(cdm$tp_cohorts)
cat("Cohort set:\n")
print(as.data.frame(cohort_set))

# Build cohort definitions matching actual IDs
cohorts <- data.frame(
  cohortId = cohort_set$cohort_definition_id,
  cohortName = cohort_set$cohort_name,
  type = ifelse(cohort_set$cohort_name == "coronary_artery", "target", "event")
)

cat("Cohort counts:\n")
print(as.data.frame(omopgenerics::cohortCount(cdm$tp_cohorts)))

result <- computePathways(
  cohorts = cohorts,
  cohortTableName = "tp_cohorts",
  cdm = cdm,
  windowStart = -9999,
  windowEnd = 9999
)

# Export results — use minCellCount = 1 (minimum allowed)
tmp_dir <- tempdir()
tryCatch({
  export(result, outputPath = tmp_dir, minCellCount = 1)
  tp_file <- file.path(tmp_dir, "treatment_pathways.csv")
  if (file.exists(tp_file)) {
    pathway_summary <- read.csv(tp_file)
  } else {
    pathway_summary <- as.data.frame(result$treatmentHistory)
  }
}, error = function(e) {
  cat("Export error:", conditionMessage(e), "\n")
  pathway_summary <<- as.data.frame(result$treatmentHistory)
})

save_result(pathway_summary, "09_treatment_patterns")
elapsed <- (proc.time() - t0)[["elapsed"]]
save_timing("09_treatment_patterns", elapsed)
cdmDisconnect(cdm)
cat("Done in", elapsed, "seconds\n")