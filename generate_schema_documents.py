from __future__ import annotations
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class SchemaDoc:
    '''
    Represents a document describing a part of the database schema.
    '''
    doc_id: str
    doc_type: str  # 'table' | 'column' | 'relationship'
    table: Optional[str]
    column: Optional[str]
    ref_table: Optional[str]
    ref_column: Optional[str]
    text: str
    meta: Dict[str, Any]


def fetch_tables(cursor: sqlite3.Cursor) -> List[str]:
    '''
    SQL querying to get all tables in the SQLite database.
    '''
    rows = cursor.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name;
    """).fetchall()
    return [r[0] for r in rows]


def fetch_column_samples(
        cursor: sqlite3.Cursor, table: str, column: str, limit: int = 3
    ) -> List[Any]:
    '''
    Gets some distinct non-null values for a column to aid semantic understanding.
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


def fetch_foreign_keys(
        cursor: sqlite3.Cursor, table: str) -> List[Dict[str, Any]]:
    '''
    SQL querying to get info about all foreign keys in a given table.
    '''
    rows = cursor.execute(
        f"""SELECT * FROM pragma_foreign_key_list('{table}')""").fetchall()
    # pragma_foreign_key_list columns:
    # id, seq, table, from, to, on_update, on_delete, match
    foreign_keys = []
    for _id, seq, ref_tab, frm_col, to_col, on_update, on_del, match in rows:
        foreign_keys.append({
            'id': int(_id),
            'seq': int(seq),
            'ref_table': ref_tab,
            'from_col': frm_col,
            'to_col': to_col,
            'on_update': on_update,
            'on_delete': on_del,
            'match': match,
        })

    return foreign_keys


def make_table_document(
        table: str, columns: List[Dict[str, Any]], 
        foreign_keys: List[Dict[str, Any]]) -> SchemaDoc:
    '''
    Makes a table document describing the table schema.
    '''
    # Column descriptions
    pk_cols = [col['name'] for col in columns if col['primary_key'] > 0]
    column_descs = []
    for column in columns:
        flags = []
        if column['primary_key'] > 0: flags.append('PRIMARY KEY')
        if column['notnull']: flags.append('NOT NULL')
        flag_str = f' [{" ".join(flags)}]' if flags else ''
        col_type = column['type'] or 'UNKNOWN'
        sample_str = ""
        if column['samples']:
            samples = [str(sample)[:50] for sample in column['samples']]
            sample_str = f" (ex: {', '.join(samples)})"
        column_desc = f' - {column["name"]} ({col_type}){flag_str}{sample_str}'
        column_descs.append(column_desc)

    # Foreign key relationship descriptions
    f_key_lines = []
    for foreign_key in foreign_keys:
        from_col = foreign_key['from_col']
        ref_table = foreign_key['ref_table']
        to_col = foreign_key['to_col']
        f_key_lines.append(f' - {table}.{from_col} → {ref_table}.{to_col}')

    # Table document description
    primary_key_text = ', '.join(pk_cols) if pk_cols else 'None'
    column_descriptors = '\n'.join(column_descs)
    foreign_key_text = '\n'.join(f_key_lines) if f_key_lines else '- None'
    table_text = (
        f'Table: {table}\n'
        f'Primary key: {primary_key_text}\n'
        f'Columns:\n{column_descriptors}\n'
        f'Foreign key(s):\n{foreign_key_text}')

    # Create table document
    document = SchemaDoc(
        doc_id=f'table:{table}',
        doc_type='table',
        table=table,
        column=None,
        ref_table=None,
        ref_column=None,
        text=table_text,
        meta={'table': table, 
            'pk_cols': pk_cols, 
            'n_cols': len(columns), 
            'n_fks': len(foreign_keys)},)
    
    return document


def make_column_document(table: str, column: Dict[str, Any]) -> SchemaDoc:
    '''
    Makes a column document describing the column schema.
    '''
    # Column description text
    column_name = column['name']
    data_type = column['type'] or 'UNKNOWN'
    is_nullable = 'no' if column['notnull'] else 'yes'
    is_primary_key = 'yes' if column['primary_key'] > 0 else 'no'
    is_default = column['default'] if column['default'] is not None else 'None'
    sample_text = "None"
    if column['samples']:
        sample_text = ", ".join([str(s) for s in column['samples']])
    col_text = (
        f'Column: {table}.{column_name}\n'
        f'Data type: {data_type}\n'
        f'Nullable: {is_nullable}\n'
        f'Primary key: {is_primary_key}\n'
        f'Default: {is_default}\n'
        f'Sample values: {sample_text}\n'
    )

    # Create column document
    document = SchemaDoc(
        doc_id=f'column:{table}.{column_name}',
        doc_type='column',
        table=table,
        column=column_name,
        ref_table=None,
        ref_column=None,
        text=col_text,
        meta={
            'table': table, 
            'column': column_name, 
            'dtype': data_type, 
            'primary_key': column['primary_key'], 
            'notnull': column['notnull']},
    )
    return document


def make_foreign_key_document(
        table: str, foreign_key: Dict[str, Any]) -> SchemaDoc:
    '''
    Makes a foreign key document describing the foreign key relationship.
    '''
    from_col = foreign_key['from_col']
    ref_table = foreign_key['ref_table']
    to_col = foreign_key['to_col']
    on_update = foreign_key['on_update']
    on_delete = foreign_key['on_delete']
    relationship_text = (
        f'Relationship: {table}.{from_col}'
        f'references {ref_table}.{to_col}\n'
        f'Join hint: JOIN {ref_table} ON '
        f'{table}.{from_col} = {ref_table}.{to_col}\n'
        f'On update: {on_update}; On delete: {on_delete}')
    document = SchemaDoc(
        doc_id=f'rel:{table}.{from_col}->{ref_table}.{to_col}',
        doc_type='relationship',
        table=table,
        column=from_col,
        ref_table=ref_table,
        ref_column=to_col,
        text=relationship_text,
        meta=dict(foreign_key, table=table))
    
    return document


def make_schema_documents(conn: sqlite3.Connection) -> List[SchemaDoc]:
    '''
    Writes documents describing the database schema.
    '''
    documents: List[SchemaDoc] = []
    cursor = conn.cursor()
    tables = fetch_tables(cursor)

    for table in tables:
        columns = fetch_table_columns(cursor, table)
        foreign_keys = fetch_foreign_keys(cursor, table)

        # ---- Table document ----
        table_document = make_table_document(
            foreign_keys=foreign_keys,
            columns=columns, 
            table=table)
        documents.append(table_document)

        # ---- Column documents ----
        for column in columns:
            column_document = make_column_document(column=column, table=table)
            documents.append(column_document)

        # ---- Relationship documents ----
        for foreign_key in foreign_keys:
            foreign_key_document = make_foreign_key_document(
                table=table,
                foreign_key=foreign_key)
            documents.append(foreign_key_document)

    return documents


if __name__ == '__main__':
    conn = sqlite3.connect('Chinook.db')
    documents = make_schema_documents(conn)
    print(f'Generated {len(documents)} schema documents')
    
    # Print sample documents
    print('Sample documents:')
    for document in documents[:3]:
        print('\n---\n', document.doc_id, '\n', document.text)