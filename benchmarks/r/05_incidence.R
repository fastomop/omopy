# Benchmark 05: Incidence
# R equivalent: IncidencePrevalence::estimateIncidence()

source("benchmarks/r/00_helpers.R")
library(IncidencePrevalence)
cat("=== 05: Incidence ===\n")
t0 <- proc.time()

con <- dbConnect(duckdb(), dbdir = DB_PATH)
cdm <- cdmFromCon(con = con, cdmSchema = "main", writeSchema = "main", cdmName = "synthea_1k")

# Generate denominator
cdm <- generateDenominatorCohortSet(
  cdm = cdm,
  name = "denominator",
  daysPriorObservation = 0
)

# Generate outcome cohort for coronary arteriosclerosis
cdm <- generateConceptCohortSet(
  cdm = cdm,
  conceptSet = list(coronary_artery = 317576),
  name = "outcome_cohort"
)

inc <- estimateIncidence(
  cdm = cdm,
  denominatorTable = "denominator",
  outcomeTable = "outcome_cohort",
  interval = "years",
  repeatedEvents = FALSE
)

save_result(as.data.frame(inc), "05_incidence")
elapsed <- (proc.time() - t0)[["elapsed"]]
save_timing("05_incidence", elapsed)
cdmDisconnect(cdm)
cat("Done in", elapsed, "seconds\n")