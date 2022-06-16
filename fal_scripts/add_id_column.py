import psycopg2
import sys
import os
sys.path.append('./fal_scripts')
from helpers import get_columns_info




ws_name = os.getenv('DATABASE_SCHEMA')
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


def add_id_column():
    col_with_types = get_columns_info(cursor, destination_table, ws_name)
    pk_id = "_priceloop_id"
    if pk_id not in [col[0] for col in col_with_types]:
        query = f"""
            ALTER TABLE "{ws_name}"."{destination_table}"
            ADD COLUMN "{pk_id}" int4 NOT NULL GENERATED ALWAYS AS IDENTITY;
        """
        cursor.execute(query)
        conn.commit()


def main():
    add_id_column()
    print("""Add ID column to "{}"."{}" """.format(ws_name, destination_table))

main()
