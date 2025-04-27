
# Packages ----------------------------------------------------------------
library(pipelineR)
library(DBI)
library(dplyr)
library(glue)
library(lubridate)

# Function ----------------------------------------------------------------
# Function are stored in this script because i don't want to store it on the package pipelineR
# PipelineR is public so it would mean providing you the solution
# However, in a 'profesional' environment, you should store the function in a package

check_logs <- function(con, 
                       timestamp = now() - hours(24), 
                       output_file = "/var/jenkins_home/my_logs/pipeline_report.txt"){
  
  query <- glue::glue_sql(
    "SELECT * FROM el_professor.pipeline_logs WHERE timestamp >= {timestamp}",
    .con = con
  )
  
  logs_tbl <- DBI::dbGetQuery(con, query)
  
  # Prepare the report content
  n_total <- nrow(logs_tbl)
  n_success <- sum(logs_tbl$status == "ok", na.rm = TRUE)
  n_failure <- sum(logs_tbl$status == "ko", na.rm = TRUE)
  
  errors <- logs_tbl |>
    dplyr::filter(status == "ko") |>
    dplyr::select(id, timestamp, message)
  
  report_text <- paste0(
    "Pipeline Monitoring Report - ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n",
    "-----------------------------------------\n",
    "Total executions (last 24 hours): ", n_total, "\n",
    "Successes: ", n_success, "\n",
    "Failures: ", n_failure, "\n",
    if (n_failure > 0) {
      paste0("\nFailures detected:\n", paste(apply(errors, 1, function(x) paste(x, collapse = " | ")), collapse = "\n"))
    } else {
      "\nAll executions successful!"
    }
  )
  
  # Write the report
  writeLines(report_text, con = output_file)
  DBI::dbDisconnect(con)
  message("Monitoring report successfully generated at: ", output_file)
}


# Code --------------------------------------------------------------------

con <- pipelineR::connect_db()
check_logs(con, 
           output_file = "test/pipeline_report.txt")
