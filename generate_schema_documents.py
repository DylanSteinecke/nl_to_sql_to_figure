from __future__ import annotations
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

@dataclass
class TableDoc:
    table_name: str
    primary_key: list[str]
    foreign_keys: list[dict[str, str]]

@dataclass
class ColumnDoc:
    '''
    Represents a column document describing a part of the database schema.
    '''
    # Vector DB Essentials
    doc_id: str
    text_description: str        
    
    # Structural Metadata 
    table_name: str
    column_name: str
    data_type: str
    
    # Primary / Foreign Key Info
    is_primary_key: bool
    is_foreign_key: bool    
    related_table: Optional[str] = None
    related_column: Optional[str] = None

    @property
    def metadata(self) -> dict:
        '''
        Helper to generate the dict format for the Vector DB insert.
        '''
        return {
            'table_name': self.table_name,
            'column_name': self.column_name,
            'data_type': self.data_type,
            'is_primary_key': self.is_primary_key,
            'is_foreign_key': self.is_foreign_key,
            'related_table': self.related_table,
            'related_column': self.related_column
        }


def fetch_tables(cursor: sqlite3.Cursor) -> List[str]:
    '''
    SQL querying to get all tables in the SQLite database.

    :param cursor: The SQLite DB cursor
    :type cursor: sqlite3.Cursor
    :return: List of table names
    :rtype: List[str]
    '''
    rows = cursor.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name;
    """).fetchall()
    return [r[0] for r in rows]


def fetch_column_samples(
        cursor: sqlite3.Cursor, table: str, column: str, limit: int = 5
    ) -> List[Any]:
    '''
    Gets some distinct non-null values for a column to aid semantic understanding.

    :param cursor: Description
    :type cursor: sqlite3.Cursor
    :param table: Description
    :type table: str
    :param column: Description
    :type column: str
    '''
    try:
        query = f'SELECT DISTINCT "{column}" FROM "{table}" WHERE "{column}" IS NOT NULL LIMIT ?'
        rows = cursor.execute(query, (limit,)).fetchall()
        return [row[0] for row in rows]
    
    except sqlite3.OperationalError:
        return []


def fetch_table_columns(
        cursor: sqlite3.Cursor, table: str) -> List[Dict[str, Any]]:
    '''
    SQL querying to get info about all columns in a given table.

    :param cursor: Description
    :type cursor: sqlite3.Cursor
    :param table: Description
    :type table: str
    '''
    rows = cursor.execute(f"""
        SELECT * FROM pragma_table_info('{table}')""").fetchall()
    # pragma_table_info columns: cid, name, type, notnull, dflt_value, pk
    cols = []
    for cid, name, ctype, notnull, dflt_value, pk in rows:
        samples = fetch_column_samples(cursor, table, name)
        cols.append({
            'cid': cid,
            'name': name,
            'type': ctype,
            'notnull': bool(notnull),
            'default': dflt_value,
            'primary_key': int(pk),
            'samples': samples,
        })
    return cols


def make_column_document(
        table: str, column: Dict[str, Any], conn: sqlite3.Connection
    ) -> ColumnDoc:
    '''
    Makes a column document object. Includes a text description suited
    for embedding in the vector database + structured metadata for 
    downstream use.

    :param table: The name of the table the column belongs to
    :type table: str
    :param column: The column info as returned by fetch_table_columns()
    :type column: Dict[str, Any]
    :return: The constructed ColumnDoc object
    '''
    # Extract column metadata
    column_name = column['name']
    data_type = column['type'] or 'UNKNOWN'

    # Create column's document text description
    header = f"Table: {table}, Column: {column_name}"
    column_samples = fetch_column_samples(
        cursor=conn.cursor(), column=column_name, table=table, limit=5)
    col_text = f"{header}. "+\
               f"Type: {data_type}."+\
               f"Sample values: {', '.join(str(s) for s in column_samples)}"

    # Create document: text + metadata
    document = ColumnDoc(
        doc_id=f'column:{table}.{column["name"]}',
        text_description=col_text,
        table_name=table,
        column_name=column['name'],
        data_type=column['type'],
        is_primary_key=column['primary_key'] > 0,
        is_foreign_key=column['is_foreign_key'],
        related_table=column.get('fk_ref_table'),
        related_column=column.get('fk_ref_column'))

    return document


def make_table_document(
        cursor: sqlite3.Cursor, table_name: str) -> TableDoc:
    '''
    Makes a table document object, storing primary key and foreign key info
    for lookup later, after retrieving relevant column documents and before
    generating the final context for SQL query generation.
        
    :param cursor: Description
    :type cursor: sqlite3.Cursor
    :param table: Description
    :type table: str
    :return: Description
    :rtype: TableDoc
    '''
    # Get Primary Key(s)
    cursor.execute(f"PRAGMA table_info({table_name})")
    pk_columns = [row[1] for row in cursor.fetchall() if row[5] > 0]

    # Get Foreign Keys
    cursor.execute(f"PRAGMA foreign_key_list({table_name})")
    foreign_keys = []
    for row in cursor.fetchall():
        to_col = row[4] if row[4] is not None else "primary_key"
        foreign_keys.append({
            "column_name": row[3],      
            "referenced_table": row[2], 
            "referenced_column": to_col})
    
    table_document = TableDoc(
        table_name=table_name,
        primary_key=pk_columns,
        foreign_keys=foreign_keys)
    
    return table_document


def make_schema_documents(
        conn: sqlite3.Connection) -> tuple[List[TableDoc], List[ColumnDoc]]:
    '''
    Writes documents describing the database schema. Column documents will
    be embedded into the vector database for retrieval later. Table documents
    will be stored in memory for lookup when generating the final context for
    SQL query generation.

    :param conn: The SQLite database connection
    :type conn: sqlite3.Connection
    :return: A tuple of (table documents, column documents)
    :rtype: tuple[List[TableDoc], List[ColumnDoc]]
    '''
    column_documents: List[ColumnDoc] = []
    table_documents: List[TableDoc] = []
    cursor = conn.cursor()
    tables = fetch_tables(cursor)

    for table in tables:
        # ---- Table documents ----
        table_document = make_table_document(cursor=cursor, table_name=table)
        table_documents.append(table_document)

        # ---- Column documents ----
        columns = fetch_table_columns(cursor, table)
        for column in columns:
            column_document = make_column_document(
                column=column, table=table, conn=conn)
            column_documents.append(column_document)

    return table_documents, column_documents


if __name__ == '__main__':
    conn = sqlite3.connect('Chinook.db')
    table_documents, column_documents = make_schema_documents(conn)
    print(f'Generated {len(table_documents) + len(column_documents)} schema documents')
    
    # Print sample documents
    print('Sample documents:')
    for document in column_documents[:3]:
        print('\n---\n', document.doc_id, '\n', document.text_description)