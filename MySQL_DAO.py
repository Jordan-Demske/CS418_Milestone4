import json
import math

import mysql.connector
from mysql.connector import errorcode
import dateutil.parser

import configparser


class MySQLConnectionManager:
    config_file = 'connection_data.conf'

    def __init__(self):
        self.config = configparser.ConfigParser()

    def __enter__(self):
        self.config.read(self.config_file)

        self.cnx = mysql.connector.connect(
            user=self.config['SQL']['user'],
            password=self.config['SQL']['password'],
            database=self.config['SQL']['database'])
        return self.cnx

    def __exit__(self, *ignore):
        print("Closing connection")
        self.cnx.close()


class MySQLCursorManager:

    def __init__(self, cnx):
        self.cursor = cnx.cursor()

    def __enter__(self):
        return self.cursor

    def __exit__(self, *ignore):
        print("Closing cursor")
        self.cursor.close()


class MySQL_DAO:
    ais_parameters = ['Class', 'MMSI']
    static_data_parameters = ['CallSign', 'Name', 'VesselType', 'CargoType', 'Length', 'Breadth', 'Draught',
                              'Destination', 'DestinationId']
    position_report_parameters = ['RoT', 'SoG', 'CoG', 'Heading', 'Status']
    position_parameters = ['type', 'coordinates']

    def format_ais_message(self, msg):
        if "Timestamp" in msg:
            date = dateutil.parser.isoparse(msg['Timestamp'])
            timestamp = date.strftime("%Y-%m-%d %H:%M:%S")
            msg['Timestamp'] = timestamp
        else:
            msg['Timestamp'] = None
        if "IMO" not in msg or msg['IMO'] == 'Unknown':
            msg['IMO'] = None
        for parameter in self.ais_parameters:
            if parameter not in msg:
                msg[parameter] = None
        return msg

    def format_position_report(self, msg):
        if "Position" in msg:
            for parameter in self.position_parameters:
                if parameter not in msg['Position']:
                    msg['Position'] = None
        if "Status" not in msg or msg['Status'] == "Unknown value":
            msg['Status'] = None
        for parameter in self.position_report_parameters:
            if parameter not in msg:
                msg[parameter] = None
        return msg

    def format_static_data(self, msg):
        if 'ETA' in msg:
            date = dateutil.parser.isoparse(msg['ETA'])
            msg['ETA'] = date.strftime("%Y-%m-%d %H:%M:%S")
        else:
            msg['ETA'] = None
        for parameter in self.static_data_parameters:
            if parameter not in msg:
                msg[parameter] = None
        return msg

    def insert_ais_batch(self, filename):
        count = 0
        with open(filename, 'r') as file:
            data = json.load(file)
            for msg in data:
                result = self.insert_ais_message(msg)
                if json.loads(result)['success'] == 1:
                    count += 1
        return json.dumps({"inserts": count})

    def insert_ais_message(self, msg):
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    msg = self.format_ais_message(msg)

                    stmt = """INSERT INTO AIS_MESSAGE(Timestamp, MMSI, Class) VALUES(%s, %s, %s);"""
                    cursor.execute(stmt, (msg['Timestamp'], msg['MMSI'], msg['Class']))
                    con.commit()

                    if "MsgType" not in msg:
                        return

                    if msg['MsgType'] == 'position_report':
                        msg = self.format_position_report(msg)
                        map_views = [None, None, None]
                        if msg['Position'] is not None:
                            for i in range(len(map_views)):
                                map_view_dict = self.get_tile(i + 1, msg['Position']['coordinates'][1],
                                                              msg['Position']['coordinates'][0])
                                cursor.execute(
                                    """SELECT * from MAP_VIEW WHERE LongitudeW = %s AND LongitudeE = %s AND LatitudeN = %s AND LatitudeS = %s;""",
                                    (map_view_dict['west'], map_view_dict['east'], map_view_dict['north'],
                                    map_view_dict['south']))
                                rs = cursor.fetchone()

                                if rs is None:
                                    continue
                                else:
                                    map_views[i] = rs[0]

                        stmt = """INSERT INTO POSITION_REPORT(AISMessage_Id, NavigationalStatus, Longitude, Latitude, RoT, SoG, CoG, Heading, LastStaticData_Id, MapView1_Id, MapView2_Id, MapView3_Id)
                                  VALUES(LAST_INSERT_ID(), %s, %s, %s, %s, %s, %s, %s, (SELECT max(STATIC_DATA.AISMessage_ID) FROM STATIC_DATA, AIS_MESSAGE WHERE STATIC_DATA.AISMessage_Id = AIS_MESSAGE.Id AND AIS_MESSAGE.MMSI = %s), %s, %s, %s);"""
                        cursor.execute(stmt, (
                            msg['Status'], msg['Position']['coordinates'][1], msg['Position']['coordinates'][0],
                            msg['RoT'],
                            msg['SoG'], msg['CoG'], msg['Heading'], msg['MMSI'], map_views[0], map_views[1],
                            map_views[2]))
                        con.commit()

                    elif msg['MsgType'] == 'static_data':
                        msg = self.format_static_data(msg)

                        if msg['IMO'] is not None:
                            stmt = """SELECT IMO FROM VESSEL WHERE IMO = %s;"""
                            cursor.execute(stmt, (msg['IMO'],))
                            rs = cursor.fetchone()
                            if rs is not None:
                                stmt = """UPDATE AIS_MESSAGE SET Vessel_IMO = %s where Id = LAST_INSERT_ID();"""
                                cursor.execute(stmt, (msg['IMO'],))
                                con.commit()

                        stmt = """INSERT INTO STATIC_DATA(AISMessage_ID, AISIMO, CallSign, Name, VesselType, CargoType, Length, Breadth, Draught, AISDestination, ETA, DestinationPort_Id)
                                  VALUES(LAST_INSERT_ID(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                        cursor.execute(stmt, (
                            msg['IMO'], msg['CallSign'], msg['Name'], msg['VesselType'], msg['CargoType'],
                            msg['Length'], msg['Breadth'], msg['Draught'], msg['Destination'],
                            msg['ETA'], msg['DestinationId']))
                        con.commit()

                    return json.dumps({"success": 1})
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            return json.dumps({"success": 0})

    def select_all_recent_positions(self):
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    # All Most Recent Positions
                    cursor.execute("""SELECT t.Id, t.MMSI, t.LatestTime, pos.Latitude, pos.Longitude
                                      FROM (SELECT Id, MMSI, max(Timestamp) as LatestTime from AIS_MESSAGE GROUP BY MMSI) t, POSITION_REPORT as pos
                                      WHERE t.Id = pos.AISMessage_Id ORDER BY t.LatestTime desc;""")
                    rs = cursor.fetchall()

                    if rs and len(rs) > 0:
                        print(rs)  # need to reformat the print statement to be something more usefull

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def select_most_recent_from_mmsi(self, mmsi):  # this might have a formating err
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    # Most recent of given MMSI - CHANGE 265866000 ------------ This is probably the wrong way to implement this vv
                    cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
                                      FROM (SELECT Id, MMSI, Vessel_IMO, max(Timestamp) from AIS_MESSAGE WHERE MMSI = %s) t, POSITION_REPORT as pos
                                      WHERE t.Id = pos.AISMessage_Id;""", mmsi)
                    rs = cursor.fetchone()

                    if rs and len(rs) > 0:
                        print(rs)  # need to reformat the print statement to be something more usefull

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def select_most_recent_5_ship_positions(self):
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    # Most recent 5 ship positions
                    cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
                                      FROM (SELECT Id, MMSI, Vessel_IMO, Timestamp FROM AIS_MESSAGE WHERE MMSI = "265866000" ORDER BY Timestamp DESC LIMIT 5) t, POSITION_REPORT as pos
                                      WHERE t.Id = pos.AISMessage_Id;""")
                    rs = cursor.fetchall()

                    if rs and len(rs) > 0:
                        print(rs)  # need to reformat the print statement to be something more usefull

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def get_tile(self, scale, long, lat):
        """
        Get the boundaries of tile of scale 2 or 3 that contains the given position.

        :param scale: zoom level (2 or 3)
        :type scale: int
        :param long: longitude
        :type long: float
        :param lat: latitude
        :type lat: float
        :return: object {'south': ... , 'north': ... , } describing the boundaries of the containing tile
        :rtype: dict
        """
        SOUTH, NORTH, WEST, EAST = (54.5, 57.5, 7.0, 13.0)
        s, n, w, e = (0.0, 0.0, 0.0, 0.0)
        if scale == 1:
            s, n, w, e = (SOUTH, NORTH, WEST, EAST)
        elif scale == 2:
            s, n, w, e = math.floor(lat * 2) / 2, math.ceil(lat * 2) / 2, math.floor(long), math.ceil(long)
        elif scale == 3:
            s, n, w, e = math.floor(lat * 4) / 4, math.ceil(lat * 4) / 4, math.floor(long * 2) / 2, math.ceil(
                long * 2) / 2

        return {'south': s, 'north': n, 'west': w, 'east': e}


# vvv --- TO DO --- vvv

'''

DONE?
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
