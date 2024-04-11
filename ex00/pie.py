import os
import psycopg2
import matplotlib.pyplot as plt
from dotenv import dotenv_values

env_vars = dotenv_values('../.env')

DB_USER = env_vars['DB_USER']
DB_PASS = env_vars['DB_PASS']
DB_NAME = env_vars['DB_NAME']
DB_HOST = env_vars['DB_HOST']
DB_PORT = env_vars['DB_PORT']

def create_pie_chart():
    table_name = 'new_customers'
    conn = psycopg2.connect(
        dbname = DB_NAME,
        user = DB_USER,
        password = DB_PASS,
        host = DB_HOST,
        port = DB_PORT
    )
    cursor = conn.cursor()
    cursor.execute("""
        select event_type, count(*) as count from {} group by event_type
    """.format(table_name))

    results = cursor.fetchall()
    cursor.close()
    conn.close()

    event_types = [result[0] for result in results]
    counts = [result[1] for result in results]

    plt.pie(counts, labels=event_types, autopct='%1.1f%%', startangle=180)
    plt.axis('equal')
    plt.show()

if __name__ == '__main__':
    create_pie_chart()