from app.db.connection import get_connection

def test():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        print("Conexão OK:", result)
        cursor.close()
        conn.close()
    except Exception as e:
        print("Erro na conexão:", e)

if __name__ == "__main__":
    test()