import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from db_connection import get_db_connection
from tabulate import tabulate

# Datenbankverbindung aufbauen und Daten abfragen
db_conn = get_db_connection()
sql_query = """
SELECT cmv.connector_pk, cmv.value_timestamp, cmv.value, cmv.measurand, cmv.unit, ts.transaction_pk 
FROM connector_meter_value AS cmv
JOIN transaction_start AS ts USING(connector_pk)
WHERE cmv.value_timestamp BETWEEN '2023-04-13 00:00:00.000000' AND '2023-04-14 00:00:00.000000'
AND ts.transaction_pk = 10
AND cmv.measurand = 'Power.Active.Import'
ORDER BY cmv.value_timestamp 
;
"""
df = pd.read_sql_query(sql_query, db_conn)
db_conn.close()

# Umwandeln value von string zu float
df['value'] = df['value'].astype(float)

# SQL-Ergebnisse ausgeben im Terminal
print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

# Plotten
plt.figure(figsize=(10, 6))
plt.plot(pd.to_datetime(df['value_timestamp']), df['value'], linestyle='-')

# x-Achse anpassen
plt.xlim(pd.Timestamp('2023-04-13'), pd.Timestamp('2023-04-14'))
# Format der Datumsangaben auf der X-Achse
ax = plt.gca()
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))

# Diagrammbeschriftung 
plt.title('Leistung über der Zeit')
plt.xlabel('Zeit')
plt.ylabel('Leistung')
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()

plt.show()