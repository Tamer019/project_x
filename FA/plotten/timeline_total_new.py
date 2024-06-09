import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.dates as mdates
from tabulate import tabulate
from db_connection import get_db_connection
import os

# parametriesierte SQL-Abfrage
# Angabe welche Transaktion betrachet werden soll
transaktion = 2941

# Angabe welcher Messwert untersucht werden soll
# messwert = 'Current.Export'
# messwert = 'Current.Import'
# messwert = 'Current.Offerend'
# messwert = 'Energy.Active.Export.Register' 
# messwert = 'Energy.Active.Import.Register' 
# messwert = 'Energy.Active.Export.Interval'
# messwert = 'Energy.Active.Import.Interval'
# messwert = 'Power.Active.Export'
messwert = 'Power.Active.Import'
# messwert = 'Power.Offerend'
# messwert = 'SoC'
# messwert = 'Temperature'
# messwert = 'Voltage'

# Zusätzliche Unterscheidung in Phase bzw. Unit
# messwert = 'Power.Active.Import'           //Phase L1, L2, L3 noch integrieren
# messwert = 'Current.Import'                //Phase L1, L2, L3 noch integrieren
# messwert = 'Voltage'                       //Phase L1, L2, L3, L1-N, L2-N, L3-N noch integrieren
# messwert = 'Energy.Active.Export.Register' //Unit Wh, kWh
# messwert = 'Energy.Active.Import.Register' //Unit Wh, kWh
# messwert = 'Energy.Active.Import.Interval' //Unit Wh, kWh
# messwert = 'Power.Offerend'                //Unit W, kW

# Datenbankverbindung aufbauen und Daten abfragen
db_conn = get_db_connection()
sql_query = """
SELECT ts.transaction_pk, ts.connector_pk, ts.start_timestamp, cmv.value_timestamp, cmv.value, cmv.measurand, cmv.unit, cs.status_timestamp, cs.status 
FROM transaction_start AS ts
JOIN connector_meter_value AS cmv USING(transaction_pk)
JOIN connector_status AS cs ON cs.connector_pk = ts.connector_pk
WHERE ts.transaction_pk = %s
AND cmv.measurand = %s
AND cs.status_timestamp >= ts.start_timestamp 
AND cs.status_timestamp <=  DATE_ADD(ts.start_timestamp, INTERVAL 5 HOUR)
ORDER BY cmv.value_timestamp 
;
"""
# Übergabe Parameter an SQL-Abfrage //%s sind Platzhalter 
df = pd.read_sql_query(sql_query, db_conn, params=(transaktion, messwert))
# df = pd.read_sql_query(sql_query, db_conn) 
db_conn.close()
   
# Daten konvertieren
df['value_timestamp'] = pd.to_datetime(df['value_timestamp'])
df['status_timestamp'] = pd.to_datetime(df['status_timestamp'])
df['value'] = df['value'].astype(float)

# SQL-Ergebnisse ausgeben im Terminal
print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

# Plotten
plt.figure(figsize=(10, 6))

# value über value_timestamp
plt.plot(df['value_timestamp'], df['value'], linestyle='-', marker='o', label='Leistung')

# status über status_timestamp (jeder Status ein Punkt auf der Y-Achse -> unique)
for status in df['status'].unique():
    status_times = df[df['status'] == status]['status_timestamp']
    plt.scatter(status_times, [df['value'].max() + 100] * len(status_times), label=status)  # +100 setzt die Punkte oberhalb der maximalen 'value'

# x-Achse Range anpassen
# plt.xlim(pd.Timestamp('2023-04-13'), pd.Timestamp('2023-04-14'))

# Format der Datumsangaben auf der X-Achse
ax = plt.gca()
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S.%f'))

# Diagrammbeschriftung 
plt.title(f'{messwert} und Status über der Zeit - Transaktion {transaktion}')
plt.xlabel('Zeit')
plt.ylabel(f'{messwert} / Status')
plt.grid(True)
plt.xticks(rotation=45)  
plt.legend()
plt.tight_layout()


# __________________________________
# Plot speichern 
speicherpfad = 'C:/Users/Tamer/Documents/Forschungsarbeit 2024/FA Python Plots'

# Überprüfen, ob der Ordner existiert. Wenn nicht, wird ein neuer Ordner erstellt
#if not os.path.exists(speicherpfad):
#    os.makedirs(speicherpfad)

# Dateiname
dateiname = f'EVETEC_Transaction{transaktion}_{messwert}.png'

# Vollständigen Pfad zum Speichern des Plots erstellen
vollständiger_pfad = os.path.join(speicherpfad, dateiname)

# Speichern des Plots an dem vollständigen Pfad
plt.savefig(vollständiger_pfad)
# ________________________________________

# Plot bei beliebiger Taste beenden
#def on_key_press(event):
#    plt.close()  # Plot schließen bei Drücken einer beliebigen Taste

# Plot bei ESC Taste beenden
def on_key_press(event):
    if event.key == 'escape':  
        plt.close()  
        
plt.gcf().canvas.mpl_connect('key_press_event', on_key_press)

plt.show()
