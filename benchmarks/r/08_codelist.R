# Benchmark 08: Codelist Generation
# R equivalent: CodelistGenerator::getCandidateCodes()

source("benchmarks/r/00_helpers.R")
library(CodelistGenerator)
cat("=== 08: Codelist ===\n")
t0 <- proc.time()

con <- dbConnect(duckdb(), dbdir = DB_PATH)
cdm <- cdmFromCon(con = con, cdmSchema = "main", writeSchema = "main", cdmName = "synthea_1k")

codes <- getCandidateCodes(
  cdm = cdm,
  keywords = "coronary",
  domains = "Condition",
  includeDescendants = TRUE
)

save_result(as.data.frame(codes), "08_codelist")
elapsed <- (proc.time() - t0)[["elapsed"]]
save_timing("08_codelist", elapsed)
cdmDisconnect(cdm)
cat("Done in", elapsed, "seconds\n")