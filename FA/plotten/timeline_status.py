import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from matplotlib.ticker import FixedLocator

from db_connection import get_db_connection

# Datenbankverbindung aufbauen
db_conn = get_db_connection()
cursor = db_conn.cursor()

# Erste SQL-Abfrage ausführen
sql_query = """
SELECT transaction_pk, t.connector_pk, start_event_timestamp, start_timestamp, stop_event_timestamp, stop_timestamp, cs.connector_pk, status_timestamp, status, vendor_id 
FROM transaction t 
INNER JOIN connector_status cs ON t.connector_pk = cs.connector_pk 
AND YEAR(cs.status_timestamp) = 2023 AND MONTH(cs.status_timestamp) = 5 AND DAY(cs.status_timestamp) = 3
WHERE t.connector_pk = 16 AND YEAR(t.stop_event_timestamp) = 2023 AND MONTH(t.stop_event_timestamp) = 5 AND DAY(t.stop_event_timestamp) = 3 
LIMIT 50;
"""
# Abfrage ausführen und Ergebnis in einen DataFrame df 
df = pd.read_sql_query(sql_query, db_conn)

# Cursor und Datenbankverbindung schließen
cursor.close()
db_conn.close()
# ____________________________________________________________________________________

# Zeitstempel formatieren
df['start_event_timestamp'] = pd.to_datetime(df['start_event_timestamp'])
df['status_timestamp'] = pd.to_datetime(df['status_timestamp'])

# Plot erstellen
plt.figure(figsize=(10, 6))
plt.title('Status über Zeit')
plt.xlabel('Timestamp')

# Für jeden Status plotten
for i, row in df.iterrows():
    y_pos = 0.2 + np.random.uniform(-0.05, 0.05)  # Y-Position zufällig generieren
    plt.plot([row['status_timestamp'], row['status_timestamp']], [y_pos + 0.1, 0], color='gray', linestyle='--', linewidth=0.5)
    plt.text(row['status_timestamp'], y_pos, row['status'], ha='center', va='bottom', fontsize=8)

# start, start_event, stop, stop_event plotten 
vars = ['start_event_timestamp','start_timestamp', 'stop_timestamp', 'stop_event_timestamp']
namen = ['start', 'start event', 'stop', 'stop event']

for i, x in enumerate(vars):
    y_pos = 0.2 + np.random.uniform(-0.05, 0.05)  # Y-Position zufällig generieren
    plt.plot([row[x], row[x]], [y_pos + 0.1, 0], color='gray', linestyle='--', linewidth=0.5)
    plt.text(row[x], y_pos, namen[i], ha='center', va='bottom', fontsize=8)
# ____________________________________________________________
   
# x-Achsenformatierung
unique_timestamps = df['status_timestamp'].dropna().unique()
unique_timestamps = mdates.date2num(unique_timestamps)  # Konvertiert datetime Objekte in Matplotlib's interne Darstellung

# Benutzerdefinierten Locator 
plt.gca().xaxis.set_major_locator(FixedLocator(unique_timestamps))

# Format für Datum und Uhrzeit 
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))

# Verbesserung der Darstellung von Datumsangaben
plt.xticks(rotation=90)

plt.tight_layout()

#__________________________________________
# Zoom vorne
"""
start_time = pd.Timestamp('2023-05-03 05:22')
end_time = pd.Timestamp('2023-05-03 05:23')
plt.xlim(start_time, end_time)
"""

# Zoom hinten
#"""
#start_time = pd.Timestamp('2023-05-03 06:17')
#end_time = pd.Timestamp('2023-05-03 06:18')
#plt.xlim(start_time, end_time)
#"""

plt.show()

