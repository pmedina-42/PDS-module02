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

def create_moustaches():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

    cursor = conn.cursor()
    cursor.execute("""
        select event_time, event_type, price, user_id from new_customers where event_type = 'purchase' order by event_time;;
    """)
    data = cursor.fetchall()
    
    purchase_counts = {}
    
    for event_time, event_type, price, user_id in data:
            year, month, day = event_time.year, event_time.month, event_time.day
            date = datetime(year, month, day)
            date_str = date.strftime('%Y-%m-%d')
            if month >= 10 or month <= 1:
                if date_str not in purchase_counts:
                    purchase_counts[date_str] = 0
                purchase_counts[date_str] += 1
    
    sorted_counts = sorted(purchase_counts.items())
    dates, counts = zip(*sorted_counts)
    
    plt.figure(figsize=(12, 8))
    plt.plot(dates, counts, linestyle='-')
    plt.ylabel("Number of customers")
    plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda x, pos: f'{int(x / 10)}'))
    tick_positions = [0, len(dates) // 4, 2 * len(dates) // 4, 3 * len(dates) // 4]
    tick_labels = ["Oct", "Nov", "Dec", "Jan"]
    plt.xticks(tick_positions, tick_labels)
    plt.xlim(dates[0], dates[-1])
    plt.show()
    
    monthly_sales = defaultdict(float)
    
    for event_time, event_type, price, user_id in data:
            year, month, day = event_time.year, event_time.month, event_time.day
            month_str = datetime(year, month, 1).strftime('%b')
            monthly_sales[month_str] += price
    
    months = ['Oct', 'Nov', 'Dec', 'Jan']
    sales = [monthly_sales[month] * 0.8 for month in months]
    
    plt.figure(figsize=(10, 6))
    plt.bar(months, sales)
    plt.ylabel("Total Sales (in Altairian Dollars)")
    plt.show()
    
    
    daily_sales = defaultdict(float)
    unique_customers = defaultdict(set)
    
    for event_time, event_type, price, user_id in data:
            date_str = event_time.strftime('%Y-%m-%d')
            daily_sales[date_str] += price
            unique_customers[date_str].add(user_id)
    
    dates = list(daily_sales.keys())
    
    average_spend_per_customer = [daily_sales[date] * 0.8 / len(unique_customers[date])
                                  for date in dates]
    
    plt.figure(figsize=(10, 6))
    plt.plot(dates, average_spend_per_customer, color='blue', alpha=0.3)
    plt.fill_between(dates, average_spend_per_customer, color='blue', alpha=0.3)
    plt.ylabel("Average Spend/Customer in A")
    tick_positions = [0, len(dates) // 4, 2 * len(dates) // 4, 3 * len(dates) // 4]
    tick_labels = ["Oct", "Nov", "Dec", "Jan"]
    plt.xticks(tick_positions, tick_labels)
    plt.yticks(np.arange(0, max(average_spend_per_customer), 5))
    plt.xlim(dates[0], dates[-1])
    plt.ylim(0)
    plt.show()

    cursor.close()
    conn.close()



if __name__ == '__main__':
    create_moustaches()
