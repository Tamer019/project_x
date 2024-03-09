from db.db_connection import get_db_connection


db_conn = get_db_connection()
cursor = db_conn.cursor()                   # SQL-Abfrage ausführen

cursor.execute("SELECT connector_pk, status_timestamp FROM connector_status WHERE status = 'SuspendedEV' ORDER BY connector_pk ASC LIMIT 1;")