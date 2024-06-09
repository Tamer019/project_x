import pandas as pd
from db_connection import get_db_connection

# Datenbankverbindung aufbauen
db_conn = get_db_connection()
cursor = db_conn.cursor()

# Ergebnis-DataFrame initialisieren
all_results = pd.DataFrame()

# Für jede Transaktion von 10 bis 20 die Abfrage ausführen
for transaction_id in range(10, 21):
    # SQL-Abfrage für die aktuelle Transaktion ausführen
    sql_query = f"""
    SELECT t.transaction_pk, t.stop_timestamp, c.value_timestamp, c.value , c.measurand, c.unit, cs.status_timestamp, cs.`status`
    FROM transaction t 
    JOIN connector_meter_value c USING(transaction_pk) 
    JOIN connector_status cs ON cs.connector_pk = c.connector_pk  
    WHERE transaction_pk = {transaction_id}
    AND c.measurand = 'Power.Active.Import'
    AND cs.`status` = 'SuspendedEV'
    AND cs.status_timestamp >= t.start_timestamp 
    AND cs.status_timestamp <=  DATE_ADD(t.start_timestamp, INTERVAL 5 HOUR)
    ORDER BY stop_timestamp ASC 
    LIMIT 100 ;
    """
    
    # Abfrage ausführen und Ergebnis in einen DataFrame laden
    df = pd.read_sql_query(sql_query, db_conn)
    
    # Daten konvertieren
    df['value_timestamp'] = pd.to_datetime(df['value_timestamp'])
    df['status_timestamp'] = pd.to_datetime(df['status_timestamp'])
    df['value'] = df['value'].astype(float)
    
    # Überprüfen, ob es Einträge mit value = 0 gibt
    zero_values = df[df['value'] == 0]

    if not zero_values.empty:
        # Finden Sie den ersten Wert von value_timestamp, bei dem der Wert 0 ist
        first_zero_index = zero_values.index[0]
        
        # Extrahieren der relevanten Zeitstempel
        result = df.loc[first_zero_index, ['status_timestamp', 'stop_timestamp', 'value_timestamp']]
        
        # Zeitstempel sortieren
        timestamps = pd.to_datetime(result)
        sorted_timestamps = timestamps.sort_values()
        
        # Zeitdifferenzen berechnen
        time_differences = sorted_timestamps.diff().dropna()
        
        # Erstellung eines DataFrames für die Ausgabe
        output_df = pd.DataFrame({
            'timestamp': sorted_timestamps,
            'time_difference': time_differences
        }).reset_index(drop=True)
        
        # Spaltennamen für die aktuelle Transaktion anpassen
        output_df.columns = [f'{transaction_id}_timestamp', f'{transaction_id}_time_difference']
        
        # DataFrame zur Gesamtergebnis-Tabelle hinzufügen
        all_results = pd.concat([all_results, output_df], axis=1)
    else:
        # Leerer DataFrame für den Fall, dass kein Eintrag mit value = 0 gefunden wurde
        output_df = pd.DataFrame({
            f'{transaction_id}_timestamp': ['Kein Eintrag mit value = 0 gefunden.'],
            f'{transaction_id}_time_difference': ['']
        })
        # DataFrame zur Gesamtergebnis-Tabelle hinzufügen
        all_results = pd.concat([all_results, output_df], axis=1)

# Cursor und Datenbankverbindung schließen
cursor.close()
db_conn.close()

# Gesamtergebnis in eine Excel-Datei schreiben
with pd.ExcelWriter('transaction_data_combined.xlsx', engine='xlsxwriter') as writer:
    all_results.to_excel(writer, index=False, sheet_name='Transactions')
    workbook = writer.book
    worksheet = writer.sheets['Transactions']

    # Format für Zeitdifferenzen definieren
    time_format = workbook.add_format({'num_format': 'hh:mm:ss'})

    # Spalten für Zeitdifferenzen anpassen
    for col_num, col_name in enumerate(all_results.columns):
        if 'time_difference' in col_name:
            worksheet.set_column(col_num, col_num, 12, time_format)
