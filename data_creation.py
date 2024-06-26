import csv
import os
import mysql.connector
from urllib.parse import urlparse
from dotenv import load_dotenv
load_dotenv()
# DB_URI example: 'mysql://user:password@localhost:3306/ecommerce'
db_uri = os.getenv("DB_URI")
# Extract components from DB_URI
scheme_removed = db_uri.split("://")[1]
user_info, host_info = scheme_removed.split("@")
user, password = user_info.split(":")
host_port, database = host_info.split("/")
host, port = host_port.split(":")

# Create db_config dictionary
db_config = {
    'host': host,
    'port': int(port),  # Convert port to integer
    'user': user,
    'password': password,
    'database': database
}

# Print extracted components (for verification)
print(host, user, password, database)

# Establish the connection
conn = mysql.connector.connect(**db_config)

cursor = conn.cursor()

# Create database if it doesn't exist
cursor.execute("CREATE DATABASE IF NOT EXISTS ecommerce")
conn.commit()
cursor.execute("USE ecommerce")

# Create tables
tables = {
    "distribution_centers": """
        CREATE TABLE IF NOT EXISTS distribution_centers(
            id INT,
            name VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT
        )
    """,
    "events": """
        CREATE TABLE IF NOT EXISTS events(
            id INT,
            user_id INT,
            sequence_number INT,
            session_id VARCHAR(255),
            created_at TIMESTAMP,
            ip_address VARCHAR(255),
            city VARCHAR(255),
            state VARCHAR(255),
            postal_code VARCHAR(255),
            browser VARCHAR(255),
            traffic_source VARCHAR(255),
            uri VARCHAR(255),
            event_type VARCHAR(255)
        )
    """,
    "inventory_items": """
        CREATE TABLE IF NOT EXISTS inventory_items(
            id INT,
            product_id INT,
            created_at TIMESTAMP,
            sold_at TIMESTAMP,
            cost FLOAT,
            product_category VARCHAR(255),
            product_name VARCHAR(255),
            product_brand VARCHAR(255),
            product_retail_price FLOAT,
            product_department VARCHAR(255),
            product_sku VARCHAR(255),
            product_distribution_center_id INT
        )
    """,
    "order_items": """
        CREATE TABLE IF NOT EXISTS order_items(
            id INT,
            order_id INT,
            user_id INT,
            product_id INT,
            inventory_item_id INT,
            status VARCHAR(255),
            created_at TIMESTAMP,
            shipped_at TIMESTAMP,
            delivered_at TIMESTAMP,
            returned_at TIMESTAMP,
            sale_price FLOAT
        )
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS orders(
            order_id INT,
            user_id INT,
            status VARCHAR(255),
            gender VARCHAR(255),
            created_at TIMESTAMP,
            returned_at TIMESTAMP,
            shipped_at TIMESTAMP,
            delivered_at TIMESTAMP,
            num_of_item INT
        )
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS products(
            id INT,
            cost FLOAT,
            category VARCHAR(255),
            name VARCHAR(255),
            brand VARCHAR(255),
            retail_price FLOAT,
            department VARCHAR(255),
            sku VARCHAR(255),
            distribution_center_id INT
        )
    """,
    "users": """
        CREATE TABLE IF NOT EXISTS users(
            id INT,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            email VARCHAR(255),
            age INT,
            gender VARCHAR(255),
            state VARCHAR(255),
            street_address VARCHAR(255),
            postal_code VARCHAR(255),
            city VARCHAR(255),
            country VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT,
            traffic_source VARCHAR(255),
            created_at TIMESTAMP
        )
    """
}

# Execute table creation queries
for table_name, create_query in tables.items():
    cursor.execute(create_query)
    conn.commit()

# Function to determine column names for each table
def get_table_columns(table_name):
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    columns = cursor.fetchall()
    return [col[0] for col in columns if col[0] != 'id']

# Insert data from CSV files
table_names = ["distribution_centers", "events", "inventory_items", "order_items", "orders", "products", "users"]

for table_name in table_names:
    with open("data/%s.csv" % table_name, "r", encoding="utf-8") as file:
        csv_data = csv.reader(file)
        next(csv_data)  # Skip headers
        counter = 0
        print("Currently inserting data into table %s" % (table_name))
        for row in csv_data:
            if counter % 10000 == 0:
                print("Progress is", counter)
            row = [None if cell == '' else cell.replace(" UTC", "") for cell in row]
            postfix = ','.join(["%s"] * len(row))
            try:
                cursor.execute("INSERT INTO %s VALUES(%s)" % (table_name, postfix), row)
            except mysql.connector.Error as err:
                print("Error occurred:", err)
            counter += 1
        conn.commit()

# Close the connection
conn.close()