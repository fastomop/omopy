# Install OHDSI R packages needed for benchmarks
# Run once: Rscript benchmarks/r/install_packages.R

options(repos = c(OHDSI = "https://ohdsi.github.io/drat", CRAN = "https://cloud.r-project.org"))

pkgs <- c(
  "CDMConnector",
  "PatientProfiles",
  "CodelistGenerator",
  "CohortCharacteristics",
  "IncidencePrevalence",
  "DrugUtilisation",
  "CohortSurvival",
  "TreatmentPatterns",
  "DrugExposureDiagnostics",
  "duckdb",
  "DBI",
  "dplyr",
  "readr"
)

for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) {
    cat("Installing", p, "...\n")
    install.packages(p)
  } else {
    cat(p, "already installed:", as.character(packageVersion(p)), "\n")
  }
}

cat("\nAll packages checked.\n")