import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import matplotlib.pyplot as plt
def connect():
    db_url = 'postgresql+psycopg2://pmedina-:mysecretpassword@localhost:5432/piscineds'
    engine = create_engine(db_url)
    return engine
try:
    engine = connect()
except SQLAlchemyError as e:
    print(f"Error: {e}")
    
def create_partitioned_table(table_name)->any:
    try:
        start_time = time.time()
        assert engine.connect(), "Error: Could not connect to the database"
        print(f"Creating partitioned table {table_name}...")
        with engine.connect() as conn:
            assert not engine.dialect.has_table(conn, table_name), f"Error: Table {table_name} already exists."
            sql_command = f"""
            CREATE TABLE {table_name} (
                event_time timestamptz,
                event_type TEXT,
                product_id INTEGER,
                price float8,
                user_id bigint,
                user_session text,
                category_id text,
                category_code TEXT,
                brand TEXT
            ) PARTITION BY RANGE (event_time);  
            """
            conn.execute(text(sql_command))
            conn.commit()
        end_time = time.time()
        print(f"Elapsed time: {end_time - start_time:.2f} seconds")             
        print(f"Table {table_name} created successfully")
        return True
    except AssertionError as e:
        print(f"Error: {e}")
        return None
def partition_table_month(name_table, name_partitioned_table, start_date, end_date):
    try:
        start_time = time.time()
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(f"DROP TABLE IF EXISTS {name_partitioned_table};"))
                conn.execute(text(f"""
                    CREATE TABLE {name_partitioned_table} PARTITION OF {name_table}
                    FOR VALUES FROM ('{start_date}') TO ('{end_date}');
                """))
                print(f"Table {name_partitioned_table} partitioned successfully")
        
        end_time = time.time()
        print(f"Elapsed time: {end_time - start_time:.2f} seconds")
    except Exception as e:
        print(f"Error: {e}")
def creta_multiple_partitions(name_table, tables_dates):
    for table_date in tables_dates:
        partition_table_month(name_table, table_date[0], table_date[1], table_date[2])
def insert_data_partitioned_table(table_origin, table_destination):
    try:
        print(f"Inserting data in {table_destination}...")
        start_time = time.time()
        with engine.connect() as conn:
            sql_command = f"""
            INSERT INTO {table_destination}
            SELECT * FROM {table_origin};
            """
            conn.execute(text(sql_command))
            conn.commit()
            print(f"Data inserted successfully in {table_destination}")
        
        end_time = time.time()
        print(f"Elapsed time: {end_time - start_time:.2f} seconds")
    except Exception as e:
        print(f"Error: {e}")
        return None
def create_index_table_partition(tables_dates, colummns)->any:
    try:
        print(f"Creating index on {colummns}...")
        start_time = time.time()
        assert engine.connect(), "Error: Could not connect to the database"
        with engine.connect() as conn:
            for table_date in tables_dates:
                sql_command = f"""
                CREATE INDEX ON {table_date[0]} ({colummns})
                """
                conn.execute(text(sql_command))
                conn.commit()
        end_time = time.time()
        print(f"Elapsed time: {end_time - start_time:.2f} seconds")
        return True
    except AssertionError as e:
        print(f"Error: {e}")
        return None
    
def rename_table():
    """
    Renames the table 'new_customers' to 'customers'
    """
    start_time = time.time()
    
    with engine.connect() as conn:
        sql_command = f"""
        DROP TABLE IF EXISTS customers;
        ALTER TABLE new_customers RENAME TO customers;
        """
        result = conn.execute(text(sql_command))
        conn.commit()
        print(f"Generated {result.rowcount} rows")
    end_time = time.time()
    print(f"Elapsed time: {end_time - start_time:.2f} seconds")
def main():
    tables_dates = [
        ("new_customers_2022_oct", "2022-10-01", "2022-11-01"),
        ("new_customers_2022_nov", "2022-11-01", "2022-12-01"),
        ("new_customers_2022_dec", "2022-12-01", "2023-01-01"),
        ("new_customers_2023_jan", "2023-01-01", "2023-02-01"),
        ("new_customers_2023_feb", "2023-02-01", "2023-03-01")
    ]
 
    next_step = create_partitioned_table("new_customers")
    if next_step != None:
        creta_multiple_partitions("new_customers", tables_dates)
        insert_data_partitioned_table("customers", "new_customers")
        create_index_table_partition(tables_dates, "event_time")
    # rename_table()
 
if __name__ == "__main__":
    main()
  
  
  
#   WHERE event_time >= '2022-10-01' AND event_time < '2023-02-01'