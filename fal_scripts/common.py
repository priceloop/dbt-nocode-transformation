def check_if_table_existed(cursor, table_schema, table_name):
    cursor.execute("select exists(select * from information_schema.tables where table_schema=%s and table_name=%s)", (table_schema, table_name,))
    return cursor.fetchone()[0]

def create_columns_table(cursor, conn, ws):
    try:
        cursor.execute(f"""CREATE TABLE {ws}.columns (
            table_name text NULL,
	        name text NULL,
            tpe text NULL,
            position int4 NULL)"""
        )
        conn.commit()
    except Exception as e:
        print(e)
        raise Exception("error")
    
def create_tables_table(cursor, conn, ws):
    try:
        cursor.execute(f"""CREATE TABLE {ws}.tables (
            name text NULL,
            primary_keys text[] NULL,
            filters text NULL
        )"""
        )
        conn.commit()
    except Exception as e:
        print(e)
        raise Exception("error")

def get_customer_table_column_details(cursor, source_table_name, ws_name):
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

def insert_new_columns_to_columns_table(cursor, conn, col_with_types, desination_table_name, ws_name):
    if not check_if_table_existed(cursor, ws_name, "columns"):
        create_columns_table(cursor, conn, ws_name)

    index = 1
    for col in col_with_types:
        query = f"""INSERT INTO "{ws_name}".columns (table_name, name, tpe, position) VALUES (%s, %s, %s, %s)
                ON CONFLICT (table_name, name) DO UPDATE
                SET name = excluded.name,
                    tpe = excluded.tpe,
                    position = excluded.position;
        """
        col_type = 'CtNumber' if col[1].lower(
        ) in ['integer', 'double precision'] else 'CtString'
        data = (desination_table_name, col[0], f'{{"{col_type}":{{}}}}', index)
        cursor.execute(query, data)
        index += 1
    
    query = f"""INSERT INTO "{ws_name}".columns (table_name, name, tpe, position) VALUES (%s, %s, %s, %s)
            ON CONFLICT (table_name, name) DO UPDATE
            SET name = excluded.name,
                tpe = excluded.tpe,
                position = excluded.position;
            """
    data = (desination_table_name, "id", '{"CtAutoId":{}}', index)
    cursor.execute(query, data)
    conn.commit()

def insert_new_table_to_tables_table(cursor, conn, desination_table_name, ws_name, unique_key):
    if not check_if_table_existed(cursor, ws_name, "tables"):
        create_tables_table(cursor, conn, ws_name)
    query = f"""INSERT INTO "{ws_name}".tables (name, primary_keys, views) VALUES (%s, %s, %s)
            ON CONFLICT (name) DO UPDATE
            SET primary_keys = excluded.primary_keys,
                views = excluded.views;
    """
    data = (desination_table_name, "{" + unique_key + "}", "[]")
    cursor.execute(query, data)
    conn.commit()
def create_data_table(cursor, conn, col_with_types, ws_name, desination_table_name):
    if not check_if_table_existed(cursor, ws_name, desination_table_name):
        columns = []
        for col in col_with_types:
            columns.append(""""{}" {} NULL""".format(col[0], col[2]))
        columns = ",\n".join(columns)
        query = f"""
            CREATE TABLE "{ws_name}"."{desination_table_name}" (
                {columns}
            );
        """
        cursor.execute(query)
        conn.commit()
    else:
        pk_id = "id"
        desination_cols = get_customer_table_column_details(cursor, desination_table_name, ws_name)
        if pk_id in [col[0] for col in desination_cols]:
            query = f"""
                ALTER TABLE "{ws_name}"."{desination_table_name}"
                DROP COLUMN "{pk_id}" CASCADE;
            """
            cursor.execute(query)
            conn.commit()