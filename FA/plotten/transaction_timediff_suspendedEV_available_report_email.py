import pandas as pd
from db_connection import get_db_connection
from datetime import timedelta
from mail_connection import send_email

# Verstoßüberprüfung, Schwellwert = 30 min = 1800 sek
def ueberpruefung(difference_seconds):
    if difference_seconds > 1800:
        return "Verstoß!"
    else:
        return "Kein Verstoß!"
    
# Datenbankverbindung aufbauen
db_conn = get_db_connection()

# Ergebnis-DataFrame initialisieren
all_results = []

transaction_start = 2850
transaction_end = 2900

# Für jede Transaktion von ... bis ... die Abfrage ausführen
for transaction_id in range(transaction_start, transaction_end):

    # SQL-Abfrage für die aktuelle Transaktion ausführen
    sql_query = f"""
    SELECT t.transaction_pk, cs.status_timestamp, cs.`status`, cs.vendor_id
    FROM transaction t 
    JOIN connector_meter_value c USING(transaction_pk) 
    JOIN connector_status cs ON cs.connector_pk = c.connector_pk  
    WHERE transaction_pk = {transaction_id}
    AND cs.vendor_id = 'EVTEC'   
    AND c.measurand = 'Power.Active.Import'
    AND cs.`status` IN ('SuspendedEV' , 'Available')
    AND cs.status_timestamp >= t.start_timestamp 
    AND cs.status_timestamp <= DATE_ADD(t.start_timestamp, INTERVAL 5 HOUR)
    ORDER BY cs.status_timestamp ASC;
    """
    # SQL-Abfrage ausführen und Ergebnis in einen DataFrame laden
    df = pd.read_sql_query(sql_query, db_conn)

    if df.empty:
        continue  # Überspringen, wenn keine Daten für die Transaktion vorhanden sind

    # Konvertieren Sie die status_timestamp-Spalte in datetime
    df['status_timestamp'] = pd.to_datetime(df['status_timestamp'])

    # Berechnen der Differenz
    try:
        suspended_time = df[df['status'] == 'SuspendedEV']['status_timestamp'].iloc[0]
        available_time = df[df['status'] == 'Available']['status_timestamp'][df['status_timestamp'] > suspended_time].iloc[0]
        difference = available_time - suspended_time

        # Unterschied in Sekunden
        difference_seconds = difference.total_seconds()

        # Unterschied im hh:mm:ss Format
        difference_str = str(difference)

        # Ergebnisse speichern
        all_results.append({
            'transaction_id': transaction_id,
            'suspended_time': suspended_time,
            'available_time': available_time,
            'difference': difference_str,
            'difference_seconds': difference_seconds,
            'Verstoß_vorliegend' : ueberpruefung(difference_seconds)
            
        })

        # Überprüfen, ob die Differenz größer als 1800 Sekunden ist
        if difference_seconds > 1800:
            print(f"Verstoß bei Transaktion {transaction_id}!")

    except IndexError:
        # Falls keine passenden Zeitstempel vorhanden sind, überspringen
        continue

# Ergebnisse in einen DataFrame umwandeln
results_df = pd.DataFrame(all_results)

# Ergebnisse in eine Excel-Datei speichern
vendor_id = 'EVTEC'  # Stellen Sie sicher, dass der vendor_id korrekt definiert ist
results_df.to_excel(f'C:/Users/Tamer/Desktop/project_x/project_x/FA/output_files/timediff_{vendor_id}_SuspendedEV_bis_Available.xlsx', index=False)

print("Verarbeitung abgeschlossen.")

# Mail wird an den Admin gesendet
send_email(
    subject="Report E-Mail",
    body="Report der Transaktion von 2850 bis 2900",
    to_email="abdelrahmanta00@gmail.com",
    from_email="abdelrahmanta00@gmail.com",
    smtp_server="smtp.gmail.com",
    smtp_port=587,
    login="abdelrahmanta00@gmail.com",
    password="qogz mjeo mrfn iwky",  # Verwenden Sie hier das generierte App-Passwort
    attachment_path="C:/Users/Tamer/Desktop/project_x/project_x/FA/output_files/timediff_EVTEC_SuspendedEV_bis_Available.xlsx"
)

