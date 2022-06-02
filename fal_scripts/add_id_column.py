import sys
sys.path.append('./fal_scripts')

import psycopg2
import os
from psycopg2.extensions import AsIs
from sqlalchemy import column
from common import get_customer_table_column_details, insert_new_table_to_tables_table
ws_name = "My first Workspace"
desination_table_name = os.getenv('DESTINATION_TABLE', 'product_csv')
DATABASE_NAME = os.getenv('DATABASE_NAME', 'dn_name')
user = os.getenv('DATABASE_USERNAME', 'user')
pwd = os.getenv('DATABASE_PASSWORD', 'password')
host = os.getenv('DATABASE_HOST', 'host')
port = int(os.getenv('DATABASE_PORT', '11111'))
connection = "dbname={} user={} password={} host={} port={:d}".format(
    DATABASE_NAME,
    user,
    pwd,
    host,
    port)

print(DATABASE_NAME, user, pwd, host, port)
conn = psycopg2.connect(connection)
cursor = conn.cursor()


def add_id_column():
    col_with_types = get_customer_table_column_details(cursor, desination_table_name, ws_name)
    pk_id = "id"
    if pk_id not in [col[0] for col in col_with_types]:
        query = f"""
            ALTER TABLE "{ws_name}"."{desination_table_name}"
            ADD COLUMN "{pk_id}" SERIAL;
        """
        cursor.execute(query)
        conn.commit()


def main():
    add_id_column()
    insert_new_table_to_tables_table(cursor, conn, desination_table_name, ws_name, "id")
    print("""Add ID column to "{}"."{}" """.format(ws_name, desination_table_name))

main()
