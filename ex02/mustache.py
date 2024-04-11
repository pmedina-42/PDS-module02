import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import dotenv_values

env_vars = dotenv_values('../.env')

DB_USER = env_vars['DB_USER']
DB_PASS = env_vars['DB_PASS']
DB_NAME = env_vars['DB_NAME']
DB_HOST = env_vars['DB_HOST']
DB_PORT = env_vars['DB_PORT']

def create_moustaches():
    table_name = 'customers'
    conn = psycopg2.connect(
        dbname = DB_NAME,
        user = DB_USER,
        password = DB_PASS,
        host = DB_HOST,
        port = DB_PORT
    )
    cursor = conn.cursor()
    query = """
        select price from {} where event_type = 'purchase'
    """.format(table_name)

    df = pd.read_sql_query(query, conn)
    results = cursor.fetchall()
    conn.close()

    plt.boxplot(df['price'], vert=False, widths=0.7)
    plt.xlabel('Price')
    plt.grid(True)
    plt.show()

if __name__ == '__main__':
    create_moustaches()