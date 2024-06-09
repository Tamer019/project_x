import pandas as pd
from db_connection import get_db_connection
from datetime import timedelta

# Datenbankverbindung aufbauen
db_conn = get_db_connection()

# Ergebnis-DataFrame initialisieren
all_results = []

transaction_start = 2850
transaction_end = 2955
vendor_id = 'EVTEC'

# Für jede Transaktion von ... bis ... die Abfrage ausführen
for transaction_id in range(transaction_start, transaction_end):
    # SQL-Abfrage für die aktuelle Transaktion ausführen
    sql_query = f"""
    SELECT t.transaction_pk, t.stop_timestamp, c.value_timestamp, c.value , c.measurand, c.unit, cs.status_timestamp, cs.`status`, cs.vendor_id
    FROM transaction t 
    JOIN connector_meter_value c USING(transaction_pk) 
    JOIN connector_status cs ON cs.connector_pk = c.connector_pk  
    WHERE transaction_pk = {transaction_id}
    AND cs.vendor_id = '{vendor_id}'   
    AND c.measurand = 'Power.Active.Import'
    AND cs.`status` = 'SuspendedEV'
    AND cs.status_timestamp >= t.start_timestamp 
    AND cs.status_timestamp <= DATE_ADD(t.start_timestamp, INTERVAL 5 HOUR)
    ORDER BY stop_timestamp ASC;
    """
    #EVTEC, ABL, MENNEKES
    
    # Abfrage ausführen und Ergebnis in einen DataFrame laden
    df = pd.read_sql_query(sql_query, db_conn)
    
    # Daten konvertieren
    df['value_timestamp'] = pd.to_datetime(df['value_timestamp'])
    df['status_timestamp'] = pd.to_datetime(df['status_timestamp'])
    df['stop_timestamp'] = pd.to_datetime(df['stop_timestamp'])
    df['value'] = df['value'].astype(float)
    
    # Überprüfen, ob es Einträge mit value = 0 gibt
    zero_values = df[df['value'] == 0]

    if not zero_values.empty:
        # Finden Sie den ersten Wert von value = 0, bei dem der vorherige Wert nicht 0 ist
        previous_values = df['value'].shift(1)
        condition = (df['value'] == 0) & (previous_values != 0) & (previous_values.notna())
        valid_zero_values = df[condition]

        if not valid_zero_values.empty:
            # Finden Sie den Index des ersten gültigen Eintrags
            first_valid_zero_index = valid_zero_values.index[0]
            
            # Extrahieren der relevanten Zeitstempel
            result = df.loc[first_valid_zero_index, ['status_timestamp', 'stop_timestamp', 'value_timestamp']]
            
            # Berechnung der Differenz zwischen 'status_timestamp' und 'value_timestamp'
            status_timestamp = result['status_timestamp']
            value_timestamp = result['value_timestamp']
            time_difference = abs(value_timestamp - status_timestamp)
            
            # Erstellung eines DataFrames für die Ausgabe
            output_df = pd.DataFrame({
                'timestamp': result,
                'time_difference': ['', '', str(timedelta(seconds=time_difference.total_seconds()))],
                'time_difference_seconds': ['', '', time_difference.total_seconds()]
            }).reset_index(drop=True)
            
            # Spaltennamen für die aktuelle Transaktion anpassen
            output_df.columns = ['timestamp', 'time_difference', 'time_difference_seconds']
            
            # Beschriftung der Timestamps
            labels = ['status_timestamp', 'stop_timestamp', 'value_timestamp']
            label_df = pd.DataFrame({'label': labels[:len(output_df)]})
            
            # DataFrame zur Gesamtergebnis-Tabelle hinzufügen
            combined_df = pd.concat([label_df, output_df], axis=1)
            combined_df = pd.concat([pd.DataFrame([[f'Transaction {transaction_id}', '', '', '']], columns=combined_df.columns), combined_df])  # Überschrift der Transaktion hinzufügen
            all_results.append(combined_df)
            all_results.append(pd.DataFrame([["", "", "", ""]], columns=combined_df.columns))  # Leere Zeile hinzufügen
        
        else:
            # Leerer DataFrame für den Fall, dass kein Eintrag mit value = 0 gefunden wurde
            output_df = pd.DataFrame({
                'timestamp': ['Kein Eintrag mit value = 0 gefunden, der davor einen Wert ungleich Null hat..'],
                'time_difference': [''],
                'time_difference_seconds': ['']
            })
            label_df = pd.DataFrame({'label': ['']})
            combined_df = pd.concat([label_df, output_df], axis=1)
            combined_df = pd.concat([pd.DataFrame([[f'Transaction {transaction_id}', '', '', '']], columns=combined_df.columns), combined_df])  # Überschrift der Transaktion hinzufügen
            all_results.append(combined_df)
            all_results.append(pd.DataFrame([["", "", "", ""]], columns=combined_df.columns))  # Leere Zeile hinzufügen
    else:
            # Leerer DataFrame für den Fall, dass kein Eintrag mit value = 0 gefunden wurde
            output_df = pd.DataFrame({
                'timestamp': ['Kein Eintrag mit value = 0 gefunden.'],
                'time_difference': [''],
                'time_difference_seconds': ['']
            })
            label_df = pd.DataFrame({'label': ['']})
            combined_df = pd.concat([label_df, output_df], axis=1)
            combined_df = pd.concat([pd.DataFrame([[f'Transaction {transaction_id}', '', '', '']], columns=combined_df.columns), combined_df])  # Überschrift der Transaktion hinzufügen
            all_results.append(combined_df)
            all_results.append(pd.DataFrame([["", "", "", ""]], columns=combined_df.columns))  # Leere Zeile hinzufügen

# Datenbankverbindung schließen
db_conn.close()

# Gesamtergebnis zusammenführen
all_results_df = pd.concat(all_results, ignore_index=True)

# Funktion zur automatischen Anpassung der Spaltenbreiten
def autofit_columns(writer, sheet_name, df):
    worksheet = writer.sheets[sheet_name]
    for idx, col in enumerate(df.columns):
        max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2  # Puffer von 2 Zeichen hinzufügen
        worksheet.set_column(idx, idx, max_len)

# Ausgabe-Dateiname
output_filename = f'C:/Users/Tamer/Desktop/project_x/project_x/FA/output_files/transaction_timediff_{vendor_id}_{transaction_start}_bis_{transaction_end}.xlsx'

# Gesamtergebnis in eine Excel-Datei schreiben
with pd.ExcelWriter(output_filename, engine='xlsxwriter') as writer:
    # Schreibe all_results_df in das Arbeitsblatt "Transactions"
    all_results_df.to_excel(writer, index=False, sheet_name='Transactions')
    autofit_columns(writer, 'Transactions', all_results_df)

writer.close()
