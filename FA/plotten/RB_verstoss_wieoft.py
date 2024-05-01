import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from db_connection import get_db_connection

def lade_und_verarbeite_daten():
    sql_query = """
    SELECT 
      t.id_tag AS id_tag,
      u.first_name AS first_name,
      u.last_name AS last_name,
      EXTRACT(YEAR FROM t.start_timestamp) AS year,
      EXTRACT(MONTH FROM t.start_timestamp) AS month,
      COUNT(t.id_tag) AS transaction_counts,
      SUM(t.stop_value - t.start_value) / 1000 AS Sum_Energy_kWh
    FROM transaction AS t
    JOIN ocpp_tag AS o ON t.id_tag = o.id_tag
    JOIN user AS u ON o.ocpp_tag_pk = u.ocpp_tag_pk
    GROUP BY t.id_tag, year, month
    HAVING COUNT(t.id_tag) >= 5
    """
    db_conn = get_db_connection()
    df = pd.read_sql_query(sql_query, db_conn)
    db_conn.close()
    return df

def speichere_tabelle_als_pdf(df, speicherpfad, dateiname):
    with PdfPages(f"{speicherpfad}/{dateiname}.pdf") as pdf:
        fig, ax = plt.subplots(figsize=(12, 8))
        ax.axis('tight')
        ax.axis('off')
        table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.2)
        plt.subplots_adjust(left=0.05, right=0.95, top=0.95, bottom=0.05) # Einstellen der Seitenränder
        pdf.savefig(fig, bbox_inches='tight')  # Sicherstellen, dass alles im Bild enthalten ist
        plt.close()

# Daten laden
df = lade_und_verarbeite_daten()

# Pfad und Dateiname für die Speicherung definieren
speicherpfad = 'C:/Users/Tamer/Documents/Forschungsarbeit 2024/FA Python Verstoß_Randbedingungen'
dateiname = 'Monatliche_Verstoeße'

# Funktion zum Speichern der Tabelle aufrufen
speichere_tabelle_als_pdf(df, speicherpfad, dateiname)
