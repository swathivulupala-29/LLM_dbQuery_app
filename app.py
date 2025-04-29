import streamlit as st
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
import pandas as pd
from groq import Groq

# Load environment variables from .env file
load_dotenv()

# Database environment variables
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_port = os.getenv("DB_PORT")

# LLM API environment
groq_api_key = os.getenv("LLM_API_KEY")
model_name = "llama-3.3-70b-versatile"  # or the correct model name
client = Groq(api_key=groq_api_key)

# Database connection
def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None

# LLM-based SQL generator
def generate_sql_query(prompt):
    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=150,
            temperature=0.5
        )
        full_response = response.choices[0].message.content.strip()

        sql_lines = []
        inside_code_block = False
        for line in full_response.splitlines():
            if line.strip().startswith("```"):
                inside_code_block = not inside_code_block
                continue
            if inside_code_block or line.strip().upper().startswith(("SELECT", "CREATE", "INSERT", "UPDATE", "DELETE")):
                sql_lines.append(line)

        sql_query = "\n".join(sql_lines).strip()
        return sql_query
    except Exception as e:
        st.error(f"Error generating SQL query: {e}")
        return None

# Execute SQL query and return results
def execute_sql_query(conn, sql_query):
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=columns)
                return df
            else:
                conn.commit()
                return "Query executed successfully (no results returned)."
    except Exception as e:
        return f"Error executing SQL query: {e}"

# Streamlit UI
st.title("LLM-powered SQL Query Generator")

user_prompt = st.text_area("Enter your query prompt", "Get all users from the test_table.")

if st.button("Generate and Run SQL Query"):
    with st.spinner("Connecting to database and generating query..."):
        conn = connect_to_db()
        if conn:
            sql_query = generate_sql_query(user_prompt)
            st.subheader("Generated SQL Query:")
            st.code(sql_query, language="sql")
            result = execute_sql_query(conn, sql_query)
            if isinstance(result, pd.DataFrame):
                st.subheader("Query Result:")
                st.dataframe(result)
            else:
                st.info(result)
            conn.close()
        else:
            st.error("Failed to connect to the database.")
