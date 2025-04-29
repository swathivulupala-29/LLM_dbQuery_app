import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_port = os.getenv("DB_PORT")

# Connect to PostgreSQL database
def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        print("Connected successfully")
        return conn
    except Exception as e:
        print("Error connecting to database:", e)
        return None

# Create a table in the database
def create_table(conn, table_name, columns, values):
    
    #Create a table in the database with the specified name and columns with values.
    # The function takes a connection object, table name, columns (as a list of tuples), and values (as a list of tuples).
    # It creates the table if it doesn't exist and inserts the values into the table.
     
    try:
        with conn.cursor() as cursor:
            # Create the SQL query to create the table
            query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(
                    sql.SQL("{} {}").format(sql.Identifier(col[0]), sql.SQL(col[1])) for col in columns
                )
            )
            cursor.execute(query)
            conn.commit()
            print(f"Table '{table_name}' created successfully.")
            # Create the SQL query to insert values into the table
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(sql.Identifier(col[0]) for col in columns if col[0] != 'id' or "SERIAL" not in col[1]),
                sql.SQL(", ").join(sql.Placeholder() for _ in values[0])
            )

            cursor.executemany(insert_query.as_string(conn), values)
            conn.commit()
            print("Data inserted successfully.")

    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()

# Close the database connection
def close_connection(conn):
    if conn:
        conn.close()
        print("Database connection closed.")

# Main execution
if __name__ == "__main__":
    table_name = "db_table"
    columns = [
        ("id", "SERIAL PRIMARY KEY"),
        ("name", "VARCHAR(100)"),
        ("age", "INTEGER"),
    ]
    values = [
    ("JohnDoe", 30),
    ("JaneSmith", 25),
    ("AliceJohnson", 28)
    ]
    # Connect to the database
    conn = connect_to_db()
    if conn:
        create_table(conn, table_name, columns, values)
        close_connection(conn)



