import mysql.connector
from mysql.connector import errorcode

import configparser

import math

import sys
import json
from dateutil import parser

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
            with open('test_input.json', 'r') as file:
                data = json.load(file)
                for msg in data:
                    date = parser.isoparse(msg['Timestamp'])
                    
                    timestamp = date.strftime("%Y-%m-%d %H:%M:%S")
                    mmsi = msg['MMSI']
                    vessel_class = msg['Class']

                    stmt = """INSERT INTO ais_message(Timestamp, MMSI, Class) VALUES(%s, %s, %s);""" 
                    cursor.execute(stmt, (timestamp, mmsi, vessel_class))
                    con.commit()
                    
                    msgType = msg['MsgType']
                    
                    if msgType == 'position_report':
                        
                        nav_status = None
                        if "NavigationalStatus" in msg:
                            nav_status = msg["NavigationalStatus"]
                        lat = msg['Position']['coordinates'][0]
                        long = msg['Position']['coordinates'][1]
                        rot = msg['RoT']
                        sog = msg['SoG']
                        cog = msg['CoG']
                        heading = msg['Heading']
                        mv1 = 1
                        mv2 = None
                        mv3 = None
                        
                        stmt = """INSERT INTO position_report(AISMessage_Id, NavigationalStatus, Longitude, Latitude, RoT, SoG, CoG, Heading, LastStaticData_Id, MapView1_Id, MapView2_Id, MapView3_Id)
                                  VALUES(LAST_INSERT_ID(), %s, %s, %s, %s, %s, %s, %s, (SELECT max(STATIC_DATA.AISMessage_ID) FROM STATIC_DATA, AIS_MESSAGE WHERE STATIC_DATA.AISMessage_Id = AIS_MESSAGE.Id AND AIS_MESSAGE.MMSI = %s), %s, %s, %s);""" 
                        cursor.execute(stmt, (nav_status, lat, long, rot, sog, cog, heading, mmsi, mv1, mv2, mv3))
                        con.commit()
                    
                    elif msgType == 'static_data':
                        
                        aisimo = msg['IMO']
                        if aisimo == 'Unknown':
                            aisimo = None
                        call_sign = msg['CallSign']
                        name = msg['Name']
                        vessel_type = msg['VesselType']
                        cargo_type = None
                        length = msg['Length']
                        breadth = msg['Breadth']
                        draught = msg['Draught']
                        ais_destination = None
                        
                        date = parser.isoparse(msg['ETA'])
                        eta = date.strftime("%Y-%m-%d %H:%M:%S")

                        destination_port_id = None
                        if "Destination" in msg:
                            destination = msg["Destination"]
                        
                        stmt = """INSERT INTO static_data(AISMessage_ID, AISIMO, CallSign, Name, VesselType, CargoType, Length, Breadth, Draught, AISDestination, ETA, DestinationPort_Id)
                                  VALUES(LAST_INSERT_ID(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""" 
                        cursor.execute(stmt, (aisimo, call_sign, name, vessel_type, cargo_type, length, breadth, draught, ais_destination, eta, destination_port_id))
                        con.commit()
            
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