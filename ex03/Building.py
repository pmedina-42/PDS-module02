import psycopg2
import matplotlib.pyplot as plt
from dotenv import dotenv_values
from collections import defaultdict
from datetime import datetime
from matplotlib.ticker import FuncFormatter
import numpy as np

env_vars = dotenv_values('../.env')

DB_USER = env_vars['DB_USER']
DB_PASS = env_vars['DB_PASS']
DB_NAME = env_vars['DB_NAME']
DB_HOST = env_vars['DB_HOST']
DB_PORT = env_vars['DB_PORT']

try:
    sql_script1 = """
        SELECT user_id, SUM(price)
        FROM customers
        WHERE event_type = 'purchase'
        GROUP BY user_id
        HAVING SUM(price) < 225;
    """
    sql_script2 = """
        SELECT user_id, COUNT(*)
        FROM customers
        WHERE event_type = 'purchase'
        GROUP BY user_id
    """

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

    cursor = conn.cursor()
    cursor.execute(sql_script1)
    data_frequency = cursor.fetchall()
    cursor.execute(sql_script2)
    data_monetary = cursor.fetchall()
    print("Data has been fetched from the table.")
    conn.commit()
    cursor.close()
    conn.close()

    frequency = [row[1] for row in data_frequency if row[1] <= 40]
    monetary = [row[1] for row in data_monetary]

    fig, axs = plt.subplots(1, 2, figsize=(15, 6))

    axs[0].grid(True, zorder=-1)
    axs[0].hist(frequency, bins=5, edgecolor='k')
    axs[0].set_ylabel('customers')
    axs[0].set_xlabel('frequency')
    axs[0].set_xticks(range(0, 39, 10))
    axs[0].set_ylim(0, 60000)
    axs[0].set_title('Frequency distribution of the number of orders per customer')

    axs[1].grid(True, zorder=-1)
    axs[1].hist(monetary, bins=5, edgecolor='k')
    axs[1].set_ylabel('Count of customers')
    axs[1].set_xlabel('Monetary value in Altairian Dollars (A$)')
    axs[1].set_title('Frequency distribution of the purchase prices per customer')

    for ax in axs:
        ax.yaxis.grid(True, linestyle='-', alpha=0.7)
        ax.set_axisbelow(True)

    plt.tight_layout()
    plt.show()

except Exception as e:
    print(f"Error: {e}")