
# Packages ----------------------------------------------------------------
library(pipelineR)
library(DBI)
library(data.table)

# Function ----------------------------------------------------------------
# Function are stored in this script because i don't want to store it on the package pipelineR
# PipelineR is public so it would mean providing you the solution
# However, in a 'profesional' environment, you should store the function in a package

checkLogs <- function(outputFile = "output.md") {
  # Connect to the database
  con <- connect_db()
  
  # Ensure disconnection even in case of error
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # Read the table
  db <- DBI::dbGetQuery(
    conn = con,
    statement = "SELECT * FROM student_alexandre.pipeline_logs ORDER BY id ASC LIMIT 100"
  ) |> data.table::as.data.table()
  
  # Define limit time
  limitTime <- Sys.time() - 24 * 3600
  
  # Filter logs from the last 24h
  db[, timestamp := as.POSIXct(timestamp)]
  db24h <- db[timestamp >= limitTime]
  
  # Keep only relevant columns
  db24h <- db24h[, .(id, symbol, status, message, timestamp)]
  
  # Compute statistics (camelCase)
  totalExecutions <- nrow(db24h)
  successfulExecutions <- db24h[status == "ok", .N]
  failedExecutions <- db24h[status != "ok", .N]
  errorMessages <- db24h[status != "ok" & !is.na(message), unique(message)]
  
  # Build Markdown report
  mdContent <- paste0(
    "---\ntitle: \"Pipeline Logs Report\"\ndate: \"", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\"\noutput: html_document\n---\n\n",
    
    "# Pipeline Logs Report (Last 24 Hours)\n\n",
    
    "## Summary\n\n",
    "- **Total number of executions:** ", totalExecutions, "\n",
    "- **Number of successful executions:** ", successfulExecutions, "\n",
    "- **Number of failed executions:** ", failedExecutions, "\n\n",
    
    "## Error Messages\n\n",
    if (length(errorMessages) == 0) {
      "- No errors recorded.\n"
    } else {
      paste0("- ", paste(errorMessages, collapse = "\n- "), "\n")
    },
    
    "\n## Detailed Logs\n\n"
  )
  
  # Generate Markdown table
  if (totalExecutions > 0) {
    header <- paste0("| ", paste(names(db24h), collapse = " | "), " |\n")
    separator <- paste0("| ", paste(rep("---", ncol(db24h)), collapse = " | "), " |\n")
    rows <- apply(db24h, 1, function(row) {
      paste0("| ", paste(row, collapse = " | "), " |")
    })
    
    tableMd <- paste0(header, separator, paste(rows, collapse = "\n"), "\n")
    mdContent <- paste0(mdContent, tableMd)
  } else {
    mdContent <- paste0(mdContent, "_No executions in the last 24 hours._\n")
  }
  
  # Write Markdown file
  writeLines(mdContent, outputFile)
  
  # Render Markdown to HTML
  htmlOutput <- sub("\\.md$", ".html", outputFile)
  
  
  rmarkdown::render(
    input = outputFile,
    output_format = "html_document",
    output_file = htmlOutput,
    quiet = TRUE
  )
  
  # file.rename("chemin/vers/fichier/source.txt", "chemin/vers/fichier/destination.txt")
  # Return the filtered data invisibly
  invisible(db24h)
}

# Code --------------------------------------------------------------------

con <- pipelineR::connect_db()
checkLogs()

