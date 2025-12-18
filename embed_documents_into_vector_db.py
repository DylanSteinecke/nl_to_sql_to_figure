'''
Embeds documents into a vector database
'''
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry
import lancedb
import pandas as pd

from generate_schema_documents import SchemaDoc

# Define the embedding function 
registry = get_registry()
lm = registry.get("sentence-transformers").create(name="all-MiniLM-L6-v2")

# Schema is defined using the LanceModel base class
class SchemaDocLanceModel(LanceModel):
    '''Store schema document text and metadata, embed the text'''
    id: str
    doc_type: str
    table: str = ""
    column: str = ""
    ref_table: str = ""
    ref_column: str = ""
    text: str = lm.SourceField() 
    vector: Vector(lm.ndims()) = lm.VectorField() # store vector here


def upsert_schema_docs_to_lancedb(
        documents: list[SchemaDoc], db_dir: str = "./lancedb", 
        table_name: str = "schema_docs"):
    '''
    Create the LanceDB table for the schema documents.
    '''
    db = lancedb.connect(db_dir)
    
    # Convert documents into dicts for LanceDB
    data = [
        {
            "id": document.doc_id,
            "doc_type": document.doc_type,
            "table": document.table or "",
            "column": document.column or "",
            "ref_table": document.ref_table or "",
            "ref_column": document.ref_column or "",
            "text": document.text,
        }
        for document in documents
    ]
    
    # Create a LanceDB table based on the schema and documents
    table = db.create_table(
        table_name, 
        schema=SchemaDocLanceModel, 
        data=data, 
        mode="overwrite")
    
    return table


def get_relevant_documents(vector_db: lancedb.table.LanceTable, query: str):
    '''
    Search the vector database for relevant schema documents
    '''
    context = 'Represent this sentence for searching relevant database schema'
    results = vector_db.search(context+query).limit(50).to_pandas()
    if not results.empty:
        top_score = results.iloc[0]['_distance'] # or 'score'?
        adaptive_threshold = top_score * 1.05
        top_results = results[results['_distance'] <= adaptive_threshold]
        return top_results
    return pd.DataFrame()

# Example usage:
# vector_db = upsert_schema_docs_to_lancedb(docs)
# print(vector_db.count_rows())
# results = vector_db.search("How is the customer table linked?").limit(5).to_pydantic(SchemaDoc)