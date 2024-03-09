#Test Ausgabe Daten aus count_transaction
#Hart kodiert, die Funktion ist nicht variabel
from db.db_connection import get_db_connection
import os

#Pfad, wo die Datei gespeichert werden soll
pfad ='C:/Users/Tamer/Documents/Forschungsarbeit 2024/Python Liste/transaction.txt'    

# Öffnen der Datei im Schreibmodus
with open(pfad, 'w') as file:
    
    db_conn = get_db_connection()
    cursor = db_conn.cursor()                   # SQL-Abfrage ausführen

    cursor.execute("SELECT * FROM count_transactions ORDER BY last_name ASC")
    rows = cursor.fetchall()

    #Überschrift der Liste
    file.write("Transaktionen\n")
    #Spalten erstellen 
    file.write("ID, Vorname, Nachname, Anzahl Transaktionen, Energieverbrauch\n")
    
    for row in rows:
        print(row)

   # Konvertierung der Zeile in einen String und Schreiben in die Datei
        file.write(str(row) + '\n')
              
    cursor.close()
    db_conn.close()
    
#Liste wird geöffnet
os.startfile(pfad)