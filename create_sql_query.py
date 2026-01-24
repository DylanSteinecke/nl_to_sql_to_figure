'''
Generate an SQL query to retrieve the data needed to satify the user's request.
To do so, this takes relevant documents and the user question as input.
'''
import pandas as pd
from ctransformers import LLM

from generate_schema_documents import ColumnDoc

FORBIDDEN_KEYWORDS = [
    "DROP TABLE", "DELETE FROM", "INSERT INTO", "UPDATE", "ALTER TABLE"]

def get_documents_text(
        documents: pd.DataFrame) -> str:
    '''
    Convert a list of ColumnDoc documents into a single context string
    :param documents: The schema documents as a DataFrame
    :type documents: pd.DataFrame
    :return: The combined context string
    :rtype: str
    '''
    context_string = ''
    for _, document in documents.iterrows():
        context_string += f'[DOCUMENT_START]\n{document.text}[DOCUMENT_END]\n\n'

    return context_string


def extract_table_descriptions(
        sql_context_df: pd.DataFrame, documents: list[ColumnDoc]) -> str:
    '''
    Extract table descriptions for the columns whose descriptions are in
    the SQL-query-generation model's context. 
    :param sql_context: The context used for SQL query generation
    :type sql_context: pd.DataFrame
    :param documents: The list of all schema documents
    :type documents: list[str]
    :return: Descriptions of the tables relevant to the columns in context
    :rtype: str 
    '''
    table_descriptions = ''

    # Store the descriptions of retrieved tables 
    existing_tables = sql_context_df[
        sql_context_df['doc_type']=='table']['table'].unique()

    # Collect descriptions for each table with a column in context
    col_sql_context = sql_context_df[sql_context_df['doc_type']=='column']
    for table_name in col_sql_context['table'].unique():
        if table_name in existing_tables:
            continue
        print(f'--- Columns for table: {table_name} ---')
        for document in documents:
            if document.doc_id == f'table:{table_name}':
                table_descriptions += f'[DOCUMENT_START]\n{document.text}[DOCUMENT_END]\n\n'
                break
    
    return table_descriptions


def generate_sql_cpu(
        question: str, retrieved_docs: str, model: LLM) -> str:
    '''
    Generates an SQL Query based on the user question and top documents
    '''
    
    prompt = """
        ### Task
        Generate a SQL query to answer [QUESTION]{question}[/QUESTION]

        ### Instructions
        - If you cannot answer the question with the available database schema, return 'I do not know'
        - Use SQLite dialect.
        - Do not alter anything in the SQL database. Only retrieve data. Do 
        not include any forbidden keywords such as {forbidden_keywords}.

        ### Database Schema
        The query will run on a database with the following schema:
        [START_SCHEMA]
        {schema}
        [END_SCHEMA]

        ### Answer
        Given the database schema, the question [QUESTION]{question}[/QUESTION] is answered with the following SQL query:
        [SQL]
        """.format(question=question,
                   schema=retrieved_docs, forbidden_keywords=FORBIDDEN_KEYWORDS)   
    print('Generating SQL...')

    response = model(prompt, max_new_tokens=400, stop=["###", ";"]) 
    
    return response


############################
### Validating SQL Query ###
############################
import sqlite3
import re

def check_forbidden_keywords(sql_query: str) -> tuple[bool, str]:
    '''
    Checks if the SQL query contains any forbidden keywords.
        
    :param sql_query: Description
    :type sql_query: str
    :return: Description
    :rtype: tuple[bool, str]
    '''
    query_upper = sql_query.upper()
    found_forbidden = [
        term for term in FORBIDDEN_KEYWORDS if term in query_upper]
    if found_forbidden:
        return False, f"query has forbidden keywords {', '.join(found_forbidden)}"
    
    return True, ""

def check_sql_schema_and_syntax(
        conn: sqlite3.Connection, sql_query: str) -> tuple[bool, str]:
    """
    Uses SQLite's EXPLAIN QUERY PLAN to validate syntax and 
    schema existence (tables/columns) without executing the query.
    
    Returns: (is_valid: bool, error_message: str)
    """
    try:
        cursor = conn.cursor()
        # EXPLAIN QUERY PLAN compiles the SQL but doesn't run it.
        # It fails if table/columns don't exist or syntax is wrong.
        cursor.execute(f"EXPLAIN QUERY PLAN {sql_query}")
        return True, None
    except sqlite3.Error as e:
        return False, str(e)