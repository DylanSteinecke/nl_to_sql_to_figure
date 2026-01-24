'''
Embeds documents into a vector database
'''
from typing import Optional

from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry
from lancedb.table import Table
import lancedb
import pandas as pd

from generate_schema_documents import ColumnDoc

# Define the embedding function 
registry = get_registry()
lm = registry.get("sentence-transformers").create(name="all-MiniLM-L6-v2")

# Schema is defined using the LanceModel base class
class SchemaDocLanceModel(LanceModel):
    '''Store schema document text and metadata, embed the text'''    
    
    # Structural Metadata 
    table_name: str
    column_name: str
    data_type: str
    
    # Primary / Foreign Key Info
    is_primary_key: bool
    is_foreign_key: bool    
    related_table: Optional[str] = None
    related_column: Optional[str] = None

    # Vector DB Essentials
    doc_id: str
    text_description: str  
    vector: Vector(lm.ndims()) = lm.VectorField() # store vector here


def upsert_schema_docs_to_lancedb(
        documents: list[ColumnDoc], db_dir: str = './lancedb', 
        table_name: str = 'schema_docs'):
    '''
    Create the LanceDB table for the schema documents.
    '''
    db = lancedb.connect(db_dir)
    
    # Convert documents into dicts for LanceDB
    data = [
        {
            "id": document.doc_id,
            "data_type": document.data_type or "",
            "table_name": document.table_name or "",
            "column_name": document.column_name or "",
            "related_table": document.related_table or "",
            "related_column": document.related_column or "",
            "text_description": document.text_description,
            "related_column": document.related_column or "",
            "is_primary_key": document.is_primary_key,
            "is_foreign_key": document.is_foreign_key,
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


def get_relevant_documents(vector_db: Table, query: str):
    '''
    Search the vector database for relevant schema documents
    '''
    context = 'Represent this sentence for searching relevant database schema'
    results = vector_db.search(context+query).limit(50).to_pandas()
    if not results.empty:
        top_score = results.iloc[0]['_distance'] # or 'score'?
        adaptive_threshold = top_score * 1.10
        top_results = results[results['_distance'] <= adaptive_threshold]
    
        return top_results
    
    return pd.DataFrame()



# Example usage:
# vector_db = upsert_schema_docs_to_lancedb(docs)
# print(vector_db.count_rows())
# results = vector_db.search("How is the customer table linked?").limit(5).to_pydantic(SchemaDoc)