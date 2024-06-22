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
    SELECT DISTINCT
        t.transaction_pk AS Transaktionsnummer, 
        u.first_name AS Vorname, 
        u.last_name AS Nachname, 
        u.e_mail AS e_mail,
        cs1.status_timestamp AS Status_Available, 
        cs2.status_timestamp AS Status_SuspendedEV,
        SEC_TO_TIME(TIMESTAMPDIFF(SECOND, cs2.status_timestamp, cs1.status_timestamp)) AS Zeitdifferenz,
        TIMESTAMPDIFF(MINUTE, cs2.status_timestamp, cs1.status_timestamp) AS Zeitdifferenz_Minuten
    FROM 
        transaction t 
        JOIN connector_meter_value c USING(transaction_pk) 
        JOIN connector_status cs1 ON cs1.connector_pk = c.connector_pk  
        JOIN connector_status cs2 ON cs1.connector_pk = cs2.connector_pk  
        JOIN ocpp_tag AS o ON t.id_tag = o.id_tag
        JOIN user AS u ON o.ocpp_tag_pk = u.ocpp_tag_pk
    WHERE 
        t.transaction_pk = {transaction_id}
        AND c.measurand = 'Power.Active.Import'
        AND cs1.status = 'Available'
        AND cs2.status = 'SuspendedEV'
        AND cs1.status_timestamp >= t.start_timestamp 
        AND cs2.status_timestamp >= t.start_timestamp 
        AND cs1.status_timestamp <= DATE_ADD(t.start_timestamp, INTERVAL 15 HOUR)
        AND cs2.status_timestamp <= DATE_ADD(t.start_timestamp, INTERVAL 15 HOUR)
        AND cs1.status_timestamp > cs2.status_timestamp
    ORDER BY 
        cs1.status_timestamp ASC;
    """
    
    
    # SQL-Abfrage ausführen und Ergebnis in einen DataFrame laden
    df = pd.read_sql_query(sql_query, db_conn)

    if df.empty:
        continue  # Überspringen, wenn keine Daten für die Transaktion vorhanden sind

    # Konvertieren status_timestamp-Spalte in datetime
    # df['status_timestamp'] = pd.to_datetime(df['status_timestamp'])

    # Berechnen der Differenz
    # try:
    #     suspended_time = df[df['status'] == 'SuspendedEV']['status_timestamp'].iloc[0]
    #     available_time = df[df['status'] == 'Available']['status_timestamp'][df['status_timestamp'] > suspended_time].iloc[0]
    #     difference = available_time - suspended_time

    #     # Unterschied in Sekunden
    #     difference_seconds = difference.total_seconds()

    #     # Unterschied im hh:mm:ss Format
    #     difference_str = str(difference)

    #     # Ergebnisse speichern
    #     all_results.append({
    #         'transaction_id': transaction_id,
    #         'suspended_time': suspended_time,
    #         'available_time': available_time,
    #         'difference': difference_str,
    #         'difference_seconds': difference_seconds,
    #         'Verstoß_vorliegend' : ueberpruefung(difference_seconds)
            
    #     })

    #     # Überprüfen, ob die Differenz größer als 1800 Sekunden ist
    #     if difference_seconds > 1800:
    #         print(f"Verstoß bei Transaktion {transaction_id}!")

    # except IndexError:
    #     # Falls keine passenden Zeitstempel vorhanden sind, überspringen
    #     continue
    
    
    # Ergebnisse speichern
    for _, row in df.iterrows():
        difference_minute = row['Zeitdifferenz_Minuten']
        all_results.append({
            'transaction_id': row['Transaktionsnummer'],
            'vorname': row['Vorname'],
            'nachname': row['Nachname'],
            'e_mail': row['e_mail'],
            'suspended_time': row['Status_SuspendedEV'],
            'available_time': row['Status_Available'],
            'difference': row['Zeitdifferenz'],
            'difference_minutes': difference_minute,
            'Verstoß_vorliegend': ueberpruefung(difference_minute)
        })    

# Ergebnisse in einen DataFrame umwandeln
results_df = pd.DataFrame(all_results)

# Sicherstellen, dass der Ordner 'output_files' existiert
output_folder = 'output_files'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Ergebnisse in eine pdf-Datei speichern
# vendor_id = 'EVTEC'  

#  # Erstellen des Textinhalts
# text_content = "Transaktionsbericht\n"
# text_content += "=" * 20 + "\n\n"

# for index, row in results_df.iterrows():
#     text_content += (f"Transaktions-ID: {row['transaction_id']}\n"
#                      f"Suspendierungszeit: {row['suspended_time']}\n"
#                      f"Verfügbarkeitszeit: {row['available_time']}\n"
#                      f"Differenz: {row['difference']}\n"
#                      f"Differenz in Sekunden: {row['difference_seconds']}\n"
#                      f"Verstoß vorliegend: {row['Verstoß_vorliegend']}\n")
#     text_content += "-" * 20 + "\n"

# # Relativer Pfad zur Textdatei
# output_file_path = os.path.join(output_folder, 'Transaktionsbericht.txt')

# # Textinhalt in die Datei schreiben
# with open(output_file_path, 'w') as file:
#     file.write(text_content)

# DataFrame in eine Excel-Datei speichern / Relativer Pfad zur Excel-Datei 
excel_file_path = os.path.join(output_folder, 'Transaktionsbericht.xlsx')
results_df.to_excel(excel_file_path, index=False)

print("Excel-Datei wurde erfolgreich gespeichert.")


#______________________________________________________________________________________________________#

# # PDF erstellen
# class PDF(FPDF):
#     def header(self):
#         self.set_font('Arial', 'B', 12)
#         self.cell(0, 10, 'Transaktionsbericht', 0, 1, 'C')

#     def footer(self):
#         self.set_y(-15)
#         self.set_font('Arial', 'I', 8)
#         self.cell(0, 10, f'Seite {self.page_no()}', 0, 0, 'C')

#     def chapter_title(self, title):
#         self.set_font('Arial', 'B', 12)
#         self.cell(0, 10, title, 0, 1, 'L')
#         self.ln(5)

#     def table(self, data):
#         # Set column widths and row height dynamically based on the number of columns
#         col_count = len(data.columns)
#         page_width = 190  # Assuming A4 size and some margin
#         col_width = page_width / col_count
#         row_height = 10

#         # Set table header
#         self.set_font('Arial', 'B', 10)
#         for column in data.columns:
#             self.cell(col_width, row_height, column, border=1)
#         self.ln(row_height)

#         # Set table body
#         self.set_font('Arial', '', 10)
#         for row in data.itertuples(index=False):
#             for cell in row:
#                 self.cell(col_width, row_height, str(cell), border=1)
#             self.ln(row_height)

# # Excel-Datei lesen und in DataFrame konvertieren
# df_from_excel = pd.read_excel(excel_file_path)

# # PDF erstellen und Tabelle hinzufügen
# pdf = PDF()
# pdf.add_page()
# pdf.chapter_title('Bericht der Transaktionen von 2850 bis 2900')
# pdf.table(df_from_excel)

# # Relativer Pfad zur PDF-Datei
# pdf_file_path = os.path.join(output_folder, 'Transaktionsbericht.pdf')
# pdf.output(pdf_file_path)

# print("PDF-Datei wurde erfolgreich gespeichert.")

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

