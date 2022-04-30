import json
import mysql.connector
from mysql.connector import errorcode

import configparser


class MySQLConnectionManager:
    
    config_file='connection_data.conf'

    def __init__(self):
        self.config=configparser.ConfigParser()
        
    def __enter__(self):
        
        self.config.read(self.config_file)
        
        self.cnx = mysql.connector.connect( 
            user=self.config['SQL']['user'],
            password=self.config['SQL']['password'], 
            database=self.config['SQL']['database'])
        return self.cnx
        
    def __exit__(self, *ignore):
        print("Closing connexion")
        self.cnx.close()


class MySQLCursorManager:

    def __init__(self, cnx): 
        self.cursor=cnx.cursor()

    def __enter__(self):
        return self.cursor

    def __exit__(self, *ignore):
        print("Closing cursor")
        self.cursor.close()

try:
    with MySQLConnectionManager() as con:
        with MySQLCursorManager( con ) as cursor:
        
            cursor.execute("""SELECT * FROM port LIMIT 5;""")
            rs = cursor.fetchall()
            
            if rs and len(rs)>0:
                print(rs) # need to reformat the print statement to be something more usefull

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)