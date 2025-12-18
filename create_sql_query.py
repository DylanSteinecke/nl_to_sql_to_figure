'''
Generate an SQL query to retrieve the data needed to satify the user's request.
To do so, this takes relevant documents and the user question as input.
'''

from ctransformers import AutoModelForCausalLM

# Load SQL language model optimized for CPU inference
model = AutoModelForCausalLM.from_pretrained(
    "MaziyarPanahi/sqlcoder-7b-2-GGUF",
    model_file="sqlcoder-7b-2.Q4_K_M.gguf", 
    model_type="mistral", 
    gpu_layers=0,  
    context_length=4096,
)

def docs_to_context_string(documents):
    '''
    Convert a list of SchemaDoc documents into a single context string
    '''
    context_string = ""
    for _, document in documents.iterrows():
        context_string += f'[DOCUMENT_START]\n{document.text}[DOCUMENT_END]\n\n'

    return context_string

def generate_sql_cpu(question, retrieved_docs):
    '''Generates an SQL Query based on the user question and top documents'''
    schema_context = docs_to_context_string(retrieved_docs)
    
    prompt = """
        ### Task
        Generate a SQL query to answer [QUESTION]{question}[/QUESTION]

        ### Instructions
        - If you cannot answer the question with the available database schema, return 'I do not know'
        - Use SQLite dialect.

        ### Database Schema
        The query will run on a database with the following schema:
        [START_SCHEMA]
        {schema}
        [END_SCHEMA]

        ### Answer
        Given the database schema, the question [QUESTION]{question}[/QUESTION] is answered with the following SQL query:
        [SQL]
        """.format(question=question, schema=schema_context)   
    print("Generating SQL...")

    response = model(prompt, max_new_tokens=400, stop=["###", ";"]) 
    
    return response
