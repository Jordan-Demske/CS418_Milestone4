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


class MySQL_DAO:

    def __select_all_recent_positions():
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager( con ) as cursor:
                    
                    #All Most Recent Positions
                    cursor.execute("""SELECT t.Id, t.MMSI, t.LatestTime, pos.Latitude, pos.Longitude
                                      FROM (SELECT Id, MMSI, max(Timestamp) as LatestTime from AIS_MESSAGE GROUP BY MMSI) t, POSITION_REPORT as pos
                                      WHERE t.Id = pos.AISMessage_Id ORDER BY t.LatestTime desc;""")
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

    def __select_most_recent_from_mmsi(mmsi):
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager( con ) as cursor:
                
                    #Most recent of given MMSI - CHANGE 265866000 ------------ This is probably the wrong way to implement this vv
                    cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
                                      FROM (SELECT Id, MMSI, Vessel_IMO, max(Timestamp) from AIS_MESSAGE WHERE MMSI = """ + mmsi + """) t, POSITION_REPORT as pos
                                      WHERE t.Id = pos.AISMessage_Id;""")
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

    def __select_most_recent_5_ship_positions():
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager( con ) as cursor:
                    
                    #Most recent 5 ship positions
                    cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO FROM (SELECT Id, MMSI, Vessel_IMO, Timestamp from AIS_MESSAGE WHERE MMSI = "265866000" ORDER BY Timestamp DESC LIMIT 5) t, POSITION_REPORT as pos WHERE t.Id = pos.AISMessage_Id;""")
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

#All Most Recent Positions
SELECT t.Id, t.MMSI, t.LatestTime, pos.Latitude, pos.Longitude
FROM (SELECT Id, MMSI, max(Timestamp) as LatestTime from AIS_MESSAGE GROUP BY MMSI) t, POSITION_REPORT as pos
WHERE t.Id = pos.AISMessage_Id ORDER BY t.LatestTime desc;

#Most recent of given MMSI CHANGE 265866000
SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
FROM (SELECT Id, MMSI, Vessel_IMO, max(Timestamp) from AIS_MESSAGE WHERE MMSI = "265866000") t, POSITION_REPORT as pos
WHERE t.Id = pos.AISMessage_Id;

#Most recent 5 ship positions
SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
FROM (SELECT Id, MMSI, Vessel_IMO, Timestamp from AIS_MESSAGE WHERE MMSI = "265866000" ORDER BY Timestamp DESC LIMIT 5) t, POSITION_REPORT as pos
WHERE t.Id = pos.AISMessage_Id;


    def insert_ais_batch():
        
        return(number_of_insertions)

    def insert_ais_message():
        INSERT INTO ais_message( Id, Timestamp, MMSI, Class, Vessel_IMO) VALUES ( %s, %s, %s, %s, %s);
        INSERT INTO position_report( AISMessage_Id, AISIMO, CallSign, Name, VesselType, CargoType, Length, Breadth, Draught, AISDestination, ETA, DestinationPort_Id) VALUES ( %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S );
        INSERT INTO static_data( AISMessage_Id, NavigationalStatus, Longitude, Latitude, RoT, SoG, CoG, Heading, LastStaticData_Id, MapView1_Id, MapView2_Id, MapView3_Id) VALUES ( %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S );



    def delete_old(currentTime, timestamp):

#    def read_all_recent_positions():

#    def read_recent_from_MMSI(mmsi):

    def read_vessel_info(mmsi, optional_criteria):



    def read_all_recent_in_tile(tile_id):

    def read_all_matching_ports(port_name, optional_country):

    def read_ship_pos_st3_given_port(port_name, country):



#    def read_last_5_positions(mmsi):



    def recent_ships_headed_to_select_port(port_id):

    def recent_ships_headed_to_any_port(port_name, country):

    def given_tile_find_contained_tiles(map_tile_id):

    def given_tile_id_get_tile(map_tile_id):

'''