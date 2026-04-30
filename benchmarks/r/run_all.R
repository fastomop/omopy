# Run all R benchmark scripts sequentially
cat("========================================\n")
cat("Running all R benchmarks\n")
cat("========================================\n\n")

scripts <- list.files("benchmarks/r", pattern = "^\\d{2}_.*\\.R$", full.names = TRUE)
scripts <- sort(scripts)

for (s in scripts) {
  cat("\n--- Running:", s, "---\n")
  tryCatch(
    source(s, local = new.env()),
    error = function(e) cat("ERROR:", conditionMessage(e), "\n")
  )
}

cat("\n========================================\n")
cat("All R benchmarks complete.\n")
cat("Results in: benchmarks/r/results/\n")
cat("========================================\n")