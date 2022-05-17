import sys
import pathlib
cur_path = pathlib.Path().resolve()
sys.path.append('./fal_scripts')


from traceback import print_tb
from types import new_class
from numpy import column_stack
import psycopg2
import os
from psycopg2.extensions import AsIs
from sqlalchemy import column
from common import get_customer_table_column_details, insert_new_columns_to_columns_table, insert_new_table_to_tables_table, create_data_table

ws_name = "My first Workspace"
source_table_name = os.getenv('SOURCE_TABLE', 'product_csv') # "product_csv"
desination_table_name = os.getenv('DESTINATION_TABLE', 'product_csv') # "product"
db_name = os.getenv('DB_NAME', 'dn_name')
user = os.getenv('USER', 'user')
pwd = os.getenv('PASSWORD', 'password')
host = os.getenv('HOST', 'host')
port = int(os.getenv('PORT', '11111'))
unique_key = os.getenv('UNIQUE_KEY', '')
connection = "dbname={} user={} password={} host={} port={:d}".format(
    db_name,
    user,
    pwd,
    host,
    port)

print(db_name, user, pwd, host, port)

conn = psycopg2.connect(connection)
cursor = conn.cursor()

def main():
    col_with_types = get_customer_table_column_details(cursor, source_table_name, "public")
    insert_new_columns_to_columns_table(cursor, conn, col_with_types, desination_table_name, ws_name)
    insert_new_table_to_tables_table(cursor, conn, desination_table_name, ws_name, unique_key)
    create_data_table(cursor, conn, col_with_types, ws_name, desination_table_name)
    print("Create or update 2 metadata tables and data table completed")

main()
    

