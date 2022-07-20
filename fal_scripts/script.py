import os
import sys
import pathlib
import psycopg2
cur_path = pathlib.Path().resolve()
sys.path.append('./fal_scripts')

from helpers import get_columns_info, insert_columns_metadata, add_table_to_tables_table, add_id_column

ws_name = os.getenv('DATABASE_SCHEMA')

source_table = os.getenv('SOURCE_TABLE')
destination_table = os.getenv('DESTINATION_TABLE')


database_name = os.getenv('DATABASE_NAME')
user = os.getenv('DATABASE_USERNAME')
pwd = os.getenv('DATABASE_PASSWORD')
host = os.getenv('DATABASE_HOST')
port = int(os.getenv('DATABASE_PORT'))

connection = "dbname={} user={} password={} host={} port={:d}".format(
    database_name,
    user,
    pwd,
    host,
    port)


conn = psycopg2.connect(connection)
cursor = conn.cursor()


def main():
    col_with_types = get_columns_info(cursor, source_table, ws_name)
    insert_columns_metadata(cursor, conn, col_with_types, destination_table, ws_name)
    add_table_to_tables_table(cursor, conn, destination_table, ws_name)
    add_id_column(cursor, conn, destination_table, ws_name)


main()
