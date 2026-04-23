# Benchmark 06: Drug Utilisation
# R equivalent: DrugUtilisation::generateIngredientCohortSet() + summariseDrugUtilisation()

source("benchmarks/r/00_helpers.R")
library(DrugUtilisation)
cat("=== 06: Drug Utilisation ===\n")
t0 <- proc.time()

con <- dbConnect(duckdb(), dbdir = DB_PATH)
cdm <- cdmFromCon(con = con, cdmSchema = "main", writeSchema = "main", cdmName = "synthea_1k")

# Generate ingredient cohort for clopidogrel (1322184) — most common drug era
cdm <- generateIngredientCohortSet(
  cdm = cdm,
  name = "drug_cohort",
  ingredient = "clopidogrel"
)

result <- summariseDrugUtilisation(cdm$drug_cohort, ingredientConceptId = 1322184)
save_result(as.data.frame(result), "06_drug_utilisation")
elapsed <- (proc.time() - t0)[["elapsed"]]
save_timing("06_drug_utilisation", elapsed)
cdmDisconnect(cdm)
cat("Done in", elapsed, "seconds\n")