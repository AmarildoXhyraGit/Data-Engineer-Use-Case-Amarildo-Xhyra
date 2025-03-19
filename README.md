After downloading car.csv and consumer.csv, I noticed data formatting issues:
Issues Identified
•	car.csv: Headers contained special characters and extra values (e.g., ('Make', 0)).
•	consumer.csv: Header misalignment – six columns of data but only five headers. The third column was labeled "Year", but it actually contained "Type" data.
Fixes Implemented
•	Cleaned Headers: Removed special characters and corrected column names.
•	Shifted Headers: Adjusted misaligned headers and renamed "Year" → "Type".
•	Handled Missing Values: If Sales Volume was missing, it was set to None to prevent database errors.
•	Converted Data: Fixed formatting issues that prevented direct manipulation.
Optimization & Execution
•	Used batch insertion (executemany) for faster database writes.
•	When Excel editing wasn’t possible, converted the file to JSON, which resolved formatting errors.
Files Overview
•	insert.py → Cleans & inserts data into PostgreSQL.
•	queries.sql → Contains SQL queries for analysis.
•	generate_graphs.py → Generates data visualizations.
