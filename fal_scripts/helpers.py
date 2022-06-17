def get_columns_info(cursor, source_table_name, ws_name):
    cursor.execute(f"""SELECT *
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = N'{source_table_name}' and TABLE_SCHEMA = N'{ws_name}' """
                   )
    all_columns = cursor.fetchall()
    cols = []
    for col in all_columns:
        # exclude airbyte generated columns
        if not col[3].startswith('_airbyte'):
            cols.append(col)
    col_with_types = []
    for col in cols:
        col_with_types.append((col[3], col[7], col[27]))
    return col_with_types


def insert_columns_metadata(cursor, conn, col_with_types, destination_table, ws_name):

    for index, col in enumerate(col_with_types):
        query = f"""INSERT INTO "{ws_name}".columns (table_name, name, tpe, position) VALUES (%s, %s, %s, %s)
                ON CONFLICT (table_name, name) DO UPDATE
                SET name = excluded.name,
                    tpe = excluded.tpe,
                    position = excluded.position;
        """
        col_type = 'CtNumber' if col[1].lower(
        ) in ['integer', 'double precision'] else 'CtString'
        data = (destination_table, col[0], f'{{"{col_type}":{{}}}}', index)
        cursor.execute(query, data)
        index += 2

    query = f"""INSERT INTO "{ws_name}".columns (table_name, name, tpe, position) VALUES (%s, %s, %s, %s)
            ON CONFLICT (table_name, name) DO UPDATE
            SET name = excluded.name,
                tpe = excluded.tpe,
                position = excluded.position;
            """
    data = (destination_table, "id", '{"CtAutoId":{}}', 1)
    cursor.execute(query, data)
    conn.commit()


def insert_table_metadata(cursor, conn, destination_table, ws_name):
    query = f"""INSERT INTO "{ws_name}".tables (name, views) VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE
            SET views = excluded.views;
    """
    data = (destination_table, "[]")
    cursor.execute(query, data)
    conn.commit()


def add_id_column(cursor, conn, destination_table, ws_name):
    col_with_types = get_columns_info(cursor, destination_table, ws_name)
    pk_id = "_priceloop_id"
    if pk_id not in [col[0] for col in col_with_types]:
        query = f"""
            ALTER TABLE "{ws_name}"."{destination_table}"
            ADD COLUMN "{pk_id}" int4 NOT NULL GENERATED ALWAYS AS IDENTITY;
        """
        cursor.execute(query)
        conn.commit()
