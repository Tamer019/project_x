from db.db_connection import get_db_connection


db_conn = get_db_connection()
cursor = db_conn.cursor()                   # SQL-Abfrage ausführen

cursor.execute("SELECT connector_pk, status_timestamp FROM connector_status WHERE status = 'SuspendedEV' ORDER BY connector_pk ASC LIMIT 1;")
p1 = cursor.fetchall()

if p1:
    # Zugriff auf das erste (und einzige) Tupel in der Liste und Entpacken in Variablen
    connector_pk, status_timestamp = p1[0]

    # Ausgabe der Werte zur Überprüfung
    print("connector_pk:", connector_pk)
    print("status_timestamp:", status_timestamp)
else:
    print("Keine Ergebnisse gefunden.")
    

cursor.execute("SELECT MIN(status_timestamp) FROM connector_status WHERE status_timestamp > %s AND status = 'Available' AND connector_pk = %s ;"), (status_timestamp, connector_pk)
row = cursor.fetchall()

cursor.close()
db_conn.close()

print(row)

