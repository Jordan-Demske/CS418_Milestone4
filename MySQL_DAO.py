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
    
    def insert_a_single_predefined_ais_message(): # need to read json data into variables
        i=1
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager( con ) as cursor:
                    msg_id = i
                    timestamp = "2020-11-18T00:00:00.000"
                    mmsi = 220490000
                    vessel_class = "Class A"
                    vessel_imo = 1000007
                    stmt = """INSERT INTO ais_message(Id, Timestamp, MMSI, Class, Vessel_IMO) VALUES(%s, %s, %s, %s, %s);""" 
                    cursor.execute(stmt, (msg_id, timestamp, mmsi, vessel_class, vessel_imo))
                    con.commit()
                    
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
    
    def __select_all_recent_positions__():
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

    def __select_most_recent_from_mmsi__(mmsi): # this might have a formating err
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager( con ) as cursor:
                    
                    #Most recent of given MMSI - CHANGE 265866000 ------------ This is probably the wrong way to implement this vv
                    cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
                                      FROM (SELECT Id, MMSI, Vessel_IMO, max(Timestamp) from AIS_MESSAGE WHERE MMSI = $mmsi) t, POSITION_REPORT as pos
                                      WHERE t.Id = pos.AISMessage_Id;""")
                    rs = cursor.fetchone()
                    
                    if rs and len(rs)>0:
                        print(rs) # need to reformat the print statement to be something more usefull

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def __select_most_recent_5_ship_positions__():
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager( con ) as cursor:
                    
                    #Most recent 5 ship positions
                    cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
                                      FROM (SELECT Id, MMSI, Vessel_IMO, Timestamp FROM AIS_MESSAGE WHERE MMSI = "265866000" ORDER BY Timestamp DESC LIMIT 5) t, POSITION_REPORT as pos
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

# vvv --- TO DO --- vvv

'''

def insert_ais_batch():
    return number of insertions

DONE?
def insert_ais_message():
    INSERT INTO ais_message( Id, Timestamp, MMSI, Class, Vessel_IMO) VALUES ( %s, %s, %s, %s, %s);
    INSERT INTO position_report( AISMessage_Id, AISIMO, CallSign, Name, VesselType, CargoType, Length, Breadth, Draught, AISDestination, ETA, DestinationPort_Id) VALUES ( %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S );
    INSERT INTO static_data( AISMessage_Id, NavigationalStatus, Longitude, Latitude, RoT, SoG, CoG, Heading, LastStaticData_Id, MapView1_Id, MapView2_Id, MapView3_Id) VALUES ( %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S );
    return(1/0 for success/failure)

def delete_old(current time, timestamp):
    return number of deletions

DONE?
def read_all_recent_positions():
    #All Most Recent Positions
    SELECT t.Id, t.MMSI, t.LatestTime, pos.Latitude, pos.Longitude
    FROM (SELECT Id, MMSI, max(Timestamp) as LatestTime from AIS_MESSAGE GROUP BY MMSI) t, POSITION_REPORT as pos
    WHERE t.Id = pos.AISMessage_Id ORDER BY t.LatestTime desc;
    return array of ship documents

DONE?
def read_recent_from_MMSI(mmsi):
    #Most recent of given MMSI CHANGE 265866000
    SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
    FROM (SELECT Id, MMSI, Vessel_IMO, max(Timestamp) from AIS_MESSAGE WHERE MMSI = "265866000") t, POSITION_REPORT as pos
    WHERE t.Id = pos.AISMessage_Id;

def read_vessel_info(mmsi, optional_criteria):
    return ship doc with relevent properties


def read_all_recent_in_tile(tile_id):
    return array of ship documents

def read_all_matching_ports(port_name, optional_country):
    return array of port documents

def read_ship_pos_st3_given_port(port_name, country):
    return if matching port, array of position documents, otherwise an array of port documents

DONE?
def read_last_5_positions(mmsi):
    #Most recent 5 ship positions
    SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
    FROM (SELECT Id, MMSI, Vessel_IMO, Timestamp from AIS_MESSAGE WHERE MMSI = "265866000" ORDER BY Timestamp DESC LIMIT 5) t, POSITION_REPORT as pos
    WHERE t.Id = pos.AISMessage_Id;
    return {mmsi: ...; Positions: [{"lat": ..., "long": ...}, ...], "IMO": ... }


def recent_ships_positions_headed_to_given_portId(port_id):
    return array of position documents

def recent_ships_positions_headed_to_given_port(port_name, country):
    return if matching port: array of of Position documents of the form {"MMSI": ..., "lat": ..., "long": ..., "IMO": ...}10 Otherwise: an Array of Port documents.

def given_tile_find_contained_tiles(map_tile_id):
    return array of tile descriptions documents

def given_tile_id_get_tile(map_tile_id):
    reutrn Binary data ( the png file )

'''