import pandas as pd
from db_connection import get_db_connection
from datetime import timedelta
# from mail_connection import send_email

# Verstoßüberprüfung, Schwellwert = 30 min = 1800 sek
def ueberpruefung(difference_seconds):
    if difference_seconds > 1800:
        return "Verstoß!"
    else:
        return "Kein Verstoß!"
    
# Datenbankverbindung aufbauen
db_conn = get_db_connection()

# Ergebnis-DataFrame initialisieren
#all_results = []

# transaction_start = 2850
# transaction_end = 2900

# Für jede Transaktion von ... bis ... die Abfrage ausführen
#for transaction_id in range(transaction_start, transaction_end):

# SQL-Abfrage für die aktuelle Transaktion ausführen
sql_query = f"""
SELECT 
  t.id_tag AS id_tag,
  u.first_name AS first_name,
  u.last_name AS last_name,
  EXTRACT(YEAR FROM t.start_timestamp) AS year,
  EXTRACT(MONTH FROM t.start_timestamp) AS month,
  COUNT(t.id_tag) AS transaction_counts,
  SUM((t.stop_value - t.start_value) / 1000) AS Sum_Energy_kWh
FROM transaction AS t
JOIN ocpp_tag AS o ON t.id_tag = o.id_tag
JOIN user AS u ON o.ocpp_tag_pk = u.ocpp_tag_pk
WHERE (t.stop_value - t.start_value) > 5000
GROUP BY t.id_tag, year, month
HAVING transaction_counts >= 5
ORDER BY last_name;
"""
# SQL-Abfrage ausführen und Ergebnis in einen DataFrame laden
df = pd.read_sql_query(sql_query, db_conn)


# Filterung auf Benutzer, die mehr als 5 Mal monatlich geladen haben
filtered_df = df[df['transaction_counts'] > 5]

# Ergebnis anzeigen
print(filtered_df)

# Ergebnisse in eine Excel-Datei speichern
output_file = 'C:/Users/Tamer/Desktop/project_x/project_x/FA/output_files/transaction_count_monthly.xlsx'
filtered_df.to_excel(output_file, index=False)

print(f"Ergebnisse wurden in {output_file} gespeichert.")