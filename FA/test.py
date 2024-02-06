from db_connection import get_db_connection

db_conn = get_db_connection()

cursor = db_conn.cursor()

cursor.execute("SELECT * FROM count_transactions")
rows = cursor.fetchall()

for row in rows:
    print(row)
    
cursor.close()
db_conn.close()
