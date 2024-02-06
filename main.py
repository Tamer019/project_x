import mysql.connector

db_connection = mysql.connector.connect(host = "localhost", user='root', passwd='FA2024', database="stevedb")

cursor = db_connection.cursor()

cursor.execute("SELECT * FROM count_transactions")
rows = cursor.fetchall()

for row in rows:
    print(row)
    
cursor.close()
db_connection.close()
