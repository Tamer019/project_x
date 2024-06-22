import pandas as pd
import os
from db_connection import get_db_connection
from datetime import timedelta
from mail_connection import send_email
from fpdf import FPDF


# notwenidige Bibliotheken: pyarrow; fastparquet

# Verstoßüberprüfung, Schwellwert = 30 min 
def ueberpruefung(difference_minute):
    if difference_minute > 30:
        return "Verstoß!"
    else:
        return "Kein Verstoß!"
    
# Datenbankverbindung aufbauen
db_conn = get_db_connection()

# Ergebnis-DataFrame initialisieren
all_results = []

transaction_start = 2850
transaction_end = 2870

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
    ORDER BY value_timestamp ASC;
    """
    
    
    # SQL-Abfrage ausführen und Ergebnis in einen DataFrame laden
    df = pd.read_sql_query(sql_query, db_conn)

    if df.empty:
        continue  # Überspringen, wenn keine Daten für die Transaktion vorhanden sind

    # Konvertieren status_timestamp-Spalte in datetime
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

# Sicherstellen, dass der Ordner 'output_files' existiert
output_folder = 'output_files'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Hersteller
vendor_id = 'EVTEC'  

 # Erstellen des Textinhalts
text_content = "Transaktionsbericht\n"
text_content += "=" * 20 + "\n\n"

for index, row in results_df.iterrows():
    text_content += (f"Transaktions-ID: {row['transaction_id']}\n"
                     f"Suspendierungszeit: {row['suspended_time']}\n"
                     f"Verfügbarkeitszeit: {row['available_time']}\n"
                     f"Differenz: {row['difference']}\n"
                     f"Differenz in Sekunden: {row['difference_seconds']}\n"
                     f"Verstoß vorliegend: {row['Verstoß_vorliegend']}\n")
    text_content += "-" * 20 + "\n"

# Relativer Pfad zur Textdatei
output_file_path = os.path.join(output_folder, 'Transaktionsbericht.txt')

# Textinhalt in die Datei schreiben
with open(output_file_path, 'w') as file:
    file.write(text_content)
    
results_df.to_excel(f'C:/Users/Tamer/Desktop/project_x/project_x/FA/output_files/timediff_{vendor_id}_SuspendedEV_bis_Available.xlsx', index=False)
    
print("Datei erfolgreich gespeichert.")


# __________________________________________________________________________________#
# # Mail wird an den Admin gesendet
# send_email(
#     subject="Report E-Mail",
#     body="Report der Transaktion von 2850 bis 2900",
#     # to_email="abdelrahmanta00@gmail.com",
#     to_email="frank.brosi@fkfs.de",
#     from_email="abdelrahmanta00@gmail.com",
#     smtp_server="smtp.gmail.com",
#     smtp_port=587,
#     login="abdelrahmanta00@gmail.com",
#     password="qogz mjeo mrfn iwky",  # generierte App-Passwort
#     attachment_path="C:/Users/Tamer/Desktop/project_x/project_x/FA/output_files/timediff_EVTEC_SuspendedEV_bis_Available.xlsx"
# )

