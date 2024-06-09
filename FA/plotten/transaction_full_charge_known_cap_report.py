import pandas as pd
from db_connection import get_db_connection
from datetime import timedelta

# Datenbankverbindung aufbauen
db_conn = get_db_connection()

# SQL-Abfrage für die aktuelle Transaktion ausführen
sql_query = """
SELECT  
    t.transaction_pk,
    (t.stop_value - t.start_value) / 1000 AS Diff_Energy_kWh,
    u.last_name, 
    v.vehicle_pk, 
    v.brand, 
    v.model, 
    v.productionYear,
    v.battery_capacity,
    EXTRACT(YEAR FROM t.start_timestamp) AS year,
    EXTRACT(MONTH FROM t.start_timestamp) AS month
FROM transaction t
JOIN ocpp_tag o USING(id_tag)
JOIN user u USING(ocpp_tag_pk)
JOIN vehicles v USING(user_pk)
ORDER BY u.user_pk ASC;
"""
# SQL-Abfrage ausführen und Ergebnis in einen DataFrame laden
df = pd.read_sql_query(sql_query, db_conn)

# Optional: Verbindung schließen, wenn sie nicht mehr benötigt wird
db_conn.close()


# Überprüfen, ob der DataFrame Daten enthält
if not df.empty:
    # Berechnung des 90% Wertes der battery_capacity
    df['Required_Energy_kWh'] = df['battery_capacity'] * 0.9 / 1000
    
    # Überprüfen, ob Diff_Energy_kWh >= 90% der battery_capacity ist
    df['Kriterium_erfüllt?'] = df['Diff_Energy_kWh'] >= df['Required_Energy_kWh']
    
    # Erstellen eines neuen DataFrame, um festzustellen, ob die Bedingung pro Monat erfüllt ist
    monthly_check = df.groupby(['last_name', 'year', 'month'])['Kriterium_erfüllt?'].any().reset_index()
    monthly_check['Kriterium_erfüllt?'] = monthly_check['Kriterium_erfüllt?'].apply(lambda x: 'Ja' if x else 'Nein')
    
    # Generieren aller Kombinationen von last_name, year und month
    all_combinations = pd.MultiIndex.from_product(
        [df['last_name'].unique(), df['year'].unique(), df['month'].unique()],
        names=['last_name', 'year', 'month']
    ).to_frame(index=False)
    
    # Zusammenführen der Kombinationen mit den monatlichen Überprüfungsergebnissen
    monthly_check = pd.merge(all_combinations, monthly_check, on=['last_name', 'year', 'month'], how='left').fillna('Nein')
    
    # Ausgabe der Ergebnisse
    print("Monatliche Überprüfung, ob das Kriterium erfüllt wurde:")
    print(monthly_check)
else:
    print("Keine Daten gefunden.")
    
# Ergebnisse in eine Excel-Datei speichern
output_file = 'C:/Users/Tamer/Desktop/project_x/project_x/FA/output_files/full_charge_known_cap.xlsx'
monthly_check.to_excel(output_file, index=False)

print(f"Ergebnisse wurden in {output_file} gespeichert.")