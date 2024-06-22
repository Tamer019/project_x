import pandas as pd
from db_connection import get_db_connection
from datetime import timedelta

# Datenbankverbindung aufbauen
db_conn = get_db_connection()

# Ergebnis-DataFrame initialisieren
# all_results = []

# transaction_start = 2850
# transaction_end = 2955
# vendor_id = 'EVTEC'

# Für jede Transaktion von ... bis ... die Abfrage ausführen
#for transaction_id in range(transaction_start, transaction_end):

# SQL-Abfrage für die aktuelle Transaktion ausführen
sql_query = """
SELECT t.transaction_pk, cs.status_timestamp, cs.`status`, cs.vendor_id
FROM transaction t 
JOIN connector_meter_value c USING(transaction_pk) 
JOIN connector_status cs ON cs.connector_pk = c.connector_pk  
WHERE transaction_pk = 2917
AND cs.vendor_id = 'EVTEC'   
AND c.measurand = 'Power.Active.Import'
AND cs.`status` IN ('SuspendedEV' , 'Available')
AND cs.status_timestamp >= t.start_timestamp 
AND cs.status_timestamp <= DATE_ADD(t.start_timestamp, INTERVAL 5 HOUR)
ORDER BY value_timestamp ASC;
"""
#EVTEC, ABL, MENNEKES

df = pd.read_sql_query(sql_query, db_conn)


# Konvertieren status_timestamp-Spalte in datetime
df['status_timestamp'] = pd.to_datetime(df['status_timestamp'])

# Berechnen der Differenz
suspended_time = df[df['status'] == 'SuspendedEV']['status_timestamp'].iloc[0]
available_time = df[df['status'] == 'Available']['status_timestamp'][df['status_timestamp'] > suspended_time].iloc[0]
difference = available_time - suspended_time

# Unterschied in Sekunden
difference_seconds = difference.total_seconds()

# Unterschied im hh:mm:ss Format
difference_str = str(difference)

# Ergebnisse anzeigen
results_df = pd.DataFrame([{
    'suspended_time': suspended_time,
    'available_time': available_time,
    'difference_hh:mm:ss': difference_str,
    'difference_seconds': difference_seconds
}])
print(results_df)

# Speichern der Ergebnisse in eine Excel-Datei
vendor_id = 'EVTEC' 
results_df.to_excel(f'C:/Users/Tamer/Desktop/project_x/project_x/FA/output_files/timediff_{vendor_id}_SuspendedEV_bis_Available.xlsx', index=False)