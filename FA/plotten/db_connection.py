import mysql.connector
from db_config import db_config

#Aufbau Verbindung mit Datenbank
def get_db_connection():
    connection = mysql.connector.connect(
        host=db_config['host'],
        user=db_config['user'],
        passwd=db_config['password'],
        database=db_config['database']
    )
    return connection