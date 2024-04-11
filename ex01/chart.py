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
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    
    # Consulta SQL para obtener el precio por cada entrada
    query = """
    SELECT user_id, price
    FROM customers
    """
    
    # Leer los resultados de la consulta directamente en un DataFrame de pandas
    df = pd.read_sql_query(query, conn)

    # Cerrar la conexión a la base de datos
    conn.close()

    # Agrupar por user_id para obtener el promedio de cada uno
    avg_price_per_user = df.groupby('user_id')['price'].mean().reset_index()

    # Preparar el plot
    plt.figure(figsize=(10, 6))

    # Dibujar el box plot para el promedio de precios de la cesta por usuario
    bp = plt.boxplot(avg_price_per_user['price'], patch_artist=True, showfliers=True, notch=True)
    
    # Cambiar color y estilo del box plot
    for box in bp['boxes']:
        box.set_facecolor('#1f77b4')

    # Añadir los precios individuales como puntos superpuestos
    plt.plot([1] * len(df['price']), df['price'], 'r.', alpha=0.2)
    
    # Añadir una línea de promedio general
    plt.axhline(df['price'].mean(), color='blue', linestyle='--', linewidth=2)

    # Ajustar el gráfico
    plt.title('Average Basket Price per User')
    plt.ylabel('Price ($)')
    plt.xticks([1], ['All Users'])  # Simplificar el eje x para representar todos los usuarios
    plt.show()

if __name__ == '__main__':
    create_moustaches()
