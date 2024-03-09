from db_connection import get_db_connection

# Datenbankverbindung aufbauen
db_conn = get_db_connection()
cursor = db_conn.cursor()

# Erste SQL-Abfrage ausführen
cursor.execute("""
    SELECT connector_pk, status_timestamp 
    FROM connector_status 
    WHERE status = 'SuspendedEV' 
    ORDER BY connector_pk ASC 
    LIMIT 1;
""")

# Ergebnis der ersten Abfrage abrufen
p1 = cursor.fetchone()

if p1:
    # Entpacken des Ergebnisses in Variablen
    connector_pk, status_timestamp = p1

    # Zweite SQL-Abfrage mit Parametern ausführen
    cursor.execute("""
        SELECT MIN(status_timestamp) 
        FROM connector_status 
        WHERE status_timestamp > %s 
        AND status = 'Available' 
        AND connector_pk = %s;
    """, (status_timestamp, connector_pk))

    # Ergebnis der zweiten Abfrage abrufen
    next_available = cursor.fetchone()

    # Ergebnis ausgeben
    if next_available and next_available[0]:
        print("Nächster verfügbarer Zeitstempel nach", status_timestamp, ":", next_available[0])
    else:
        print("Kein weiterer Eintrag gefunden.")

else:
    print("Keine Ergebnisse gefunden für den Status 'SuspendedEV'.")

# Cursor und Datenbankverbindung schließen
cursor.close()
db_conn.close()