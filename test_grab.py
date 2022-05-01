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


'''
#{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":220490000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[56.60658,8.088727]},"Status":"Under way using engine","RoT":35,"SoG":8.8,"CoG":12.2,"Heading":16},
#{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":210169000,"MsgType":"static_data","IMO":9584865,"CallSign":"5BNZ3","Name":"KATHARINA SCHEPERS","VesselType":"Cargo","CargoTye":"Category X","Length":152,"Breadth":24,"Draught":7.8,"Destination":"NODRM","ETA":"2020-11-18T09:00:00.000Z","A":143,"B":9,"C":13,"D":11},

json_data = '{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":220490000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[56.60658,8.088727]},"Status":"Under way using engine","RoT":35,"SoG":8.8,"CoG":12.2,"Heading":16}'


    def insert_msg(self, json_data):
        msg_id, timestamp, mmsi, vessel_class, vessel_imo = extract( json.parse( json_data ))
        with MySQLConnectionManager() as cnx:
            cursor = cnx.cursor(prepared=True)
            stmt = """INSERT INTO TABLE PositionReport VALUES (%s, %s,%s, %s)""" 
            cursor.execute(stmt, (msg_id, timestamp, mmsi, lat, long))
'''