# cloudfunctionduckdb
run DuckDB in Cloud function, use SQL Query as an input
on startup, the data is prefeched to Cloud Run using the new Google Cloud Sorage Transfer manager
Currently, there is a bug where Cloud Run allocate Only 1 CPU
https://stackoverflow.com/questions/75431182/google-cloud-functions-uses-only-1-cpu-regardless-of-allocation

