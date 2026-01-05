(Work in progress / Incomplete)
This pipeline is intended for the use case where there is an SQL database and a user wants to know some summary about the data. From a technical perspective, this takes in a natural language query (e.g., "Which artist sold the most records last year?"), embeds it in the same space as automatatically generated SQL column descriptions are embedded, retrieves the most similar column descriptions, provides these column descriptions plus the original user query to a NL-to-SQL language model, generates an SQL query to retrieve that data (Done but query is unreliable), and determines which code to run or generate for the analysis needed to satisfy the user's original question. 


To Do:
- Better define the most relevant documents to retrieve (e.g., re-rank the top results, pick a better threshold or dynamically set it)
- Implemente guardrails for SQL query safety and accuracy (e.g., explore agentic approach to iterate on the query, improve document descriptions)
- Implement data-to-analysis part of the pipeline
