import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def get_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("SUPABASE_HOST"),
            port=os.getenv("SUPABASE_PORT"),
            dbname=os.getenv("SUPABASE_DB"),
            user=os.getenv("SUPABASE_USER"),
            password=os.getenv("SUPABASE_PASSWORD")
        )
        return conn
    except Exception as e:
        print("Erro ao conectar no banco:", e)
        raise