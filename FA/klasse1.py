from db_connection import get_db_connection

def get_table(table):
    if table == "Addresse":
        table = "`address`"
        return table
    elif table == "Ladesäule":
        table = "`charge_box`"
        return table
    elif table == "Park":
        table = "`charge_park`"
        return table
    elif table == "Authorisation":
        table = "`charge_park_authorization`"
        return table
    elif table == "Ladestruktur_Wallbox":
        table = "`charging_profile`"
        return table
    elif table == "Anschluss_Wallbox":
        table = "connector"
        return table
    elif table == "Profil_Anschluss":
        table = "connector_charging_profile"
        return table
    elif table == "Status_Wallbox":
        table = "connector_status"
        return table
    elif table == "Zählung":
        table = "count_transactions"
        return table
    elif table == "Gruppen":
        table = "groups"
        return table
    elif table == "Historie_OCPP":
        table = "ocpp_history"
        return table
    elif table == "Datenkarte_OCPP":
        table = "ocpp_tag"
        return table
    
def request_table(table):
    get_table(table)
    
    db_conn = get_db_connection()
    
    cursor = db_conn.cursor()
    
    cursor.execute("SELECT * FROM " + table)
    rows = cursor.fetchall()

    for row in rows:
        print(row)
    
    cursor.close()
    db_conn.close()


    
