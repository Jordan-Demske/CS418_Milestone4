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
        self.cnx.close()


class MySQLCursorManager:

    def __init__(self, cnx):
        self.cursor = cnx.cursor(buffered=True)

    def __enter__(self):
        return self.cursor

    def __exit__(self, *ignore):
        self.cursor.close()


class MySQL_DAO:
    ais_parameters = ['Class', 'MMSI']
    static_data_parameters = ['CallSign', 'Name', 'VesselType', 'CargoType', 'Length', 'Breadth', 'Draught',
                              'Destination', 'DestinationId']
    position_report_parameters = ['RoT', 'SoG', 'CoG', 'Heading']
    position_parameters = ['type', 'coordinates']

    def __init__(self, stub=False):
        self.is_stub = stub

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

    def insert_ais_batch(self, json_data):
        count = 0
        try:
            data = json.loads(json_data)
        except Exception as e:
            print(e)
            return -1
        if not isinstance(data, list):
            print("Input of `insert_ais_batch` must be a JSON array of message objects.")
            return -1
        if self.is_stub:
            return json.dumps({"inserts": len(data)})
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
                    if self.is_stub:
                        if msg['MsgType'] == 'position_report':
                            return 'pos'
                        else:
                            return 'stat'
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
                                  VALUES(LAST_INSERT_ID(), %s, %s, %s, %s, %s, %s, %s, (SELECT MAX(STATIC_DATA.AISMessage_ID) FROM STATIC_DATA, AIS_MESSAGE WHERE STATIC_DATA.AISMessage_Id = AIS_MESSAGE.Id AND AIS_MESSAGE.MMSI = %s), %s, %s, %s);"""
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

    def delete_old_ais_messages(self):
        if self.is_stub:
            return True
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute(
                        """DELETE POSITION_REPORT FROM POSITION_REPORT JOIN AIS_MESSAGE ON AISMessage_ID = AIS_MESSAGE.Id WHERE AIS_MESSAGE.Timestamp < (CURRENT_TIMESTAMP - INTERVAL 5 MINUTE);""")
                    cursor.execute(
                        """DELETE STATIC_DATA FROM STATIC_DATA JOIN AIS_MESSAGE ON AISMessage_ID = AIS_MESSAGE.Id WHERE AIS_MESSAGE.Timestamp < (CURRENT_TIMESTAMP - INTERVAL 5 MINUTE);""")
                    cursor.execute(
                        """DELETE FROM AIS_MESSAGE WHERE Timestamp < (CURRENT_TIMESTAMP - INTERVAL 5 MINUTE);""")
                    deletions = cursor.rowcount
                    con.commit()
                    return json.dumps({"deletions": deletions})

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def get_vessel_imo(self, mmsi):
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute("""SELECT IMO FROM VESSEL WHERE MMSI = %s;""", (mmsi,))
                    rows = cursor.fetchall()
                    if cursor.rowcount > 0:
                        return rows[0][0]
                    else:
                        return "NULL"

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def get_vessel_name(self, mmsi):
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute("""SELECT Name FROM VESSEL WHERE MMSI = %s;""", (mmsi,))
                    rows = cursor.fetchall()
                    if cursor.rowcount > 0:
                        return rows[0][0]
                    else:
                        return "NULL"

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def get_optional_vessel_data(self, mmsi):
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    data = ["NULL", "NULL"]
                    cursor.execute(
                        """SELECT Name, AISIMO FROM STATIC_DATA, AIS_MESSAGE WHERE AISMessage_Id = Id AND MMSI = %s ORDER BY Timestamp DESC;""",
                        (mmsi,))
                    rows = cursor.fetchall()
                    for row in rows:
                        if row[0] is not None:
                            data[0] = row[0]
                        if row[1] is not None:
                            data[1] = row[1]
            if data[0] == "NULL":
                data[0] = self.get_vessel_name(mmsi)
            if data[1] == "NULL":
                data[1] = self.get_vessel_imo(mmsi)
            return data

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def create_vessel_document(self, vessel):
        optional_data = self.get_optional_vessel_data(vessel[0])
        return {
            "MMSI": vessel[0],
            "Latitude": float(vessel[1]),
            "Longitude": float(vessel[2]),
            "Name": optional_data[0],
            "IMO": optional_data[1]
        }

    def select_all_recent_positions(self):
        if self.is_stub:
            return True
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude
                                      FROM (SELECT Id, MMSI, MAX(Timestamp) as LatestTime from AIS_MESSAGE GROUP BY MMSI) t, POSITION_REPORT as pos
                                      WHERE t.Id = pos.AISMessage_Id ORDER BY t.LatestTime DESC;""")
                    rows = cursor.fetchall()
                    vessels = []
                    if len(rows) > 0:
                        for row in rows:
                            vessel = self.create_vessel_document(row)
                            vessels.append(vessel)

                    return json.dumps({"vessels": vessels})

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def select_most_recent_from_mmsi(self, mmsi):
        if self.is_stub:
            return True
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
                                      FROM (SELECT Id, MMSI, Vessel_IMO, Timestamp from AIS_MESSAGE WHERE MMSI = %s ORDER BY Timestamp DESC) t, POSITION_REPORT as pos
                                      WHERE t.Id = pos.AISMessage_Id;""", (mmsi,))
                    if cursor.rowcount == 0:
                        return json.dumps({})
                    pos = cursor.fetchall()[0]
                    cursor.execute(
                        """SELECT Vessel_IMO, MMSI, Timestamp FROM AIS_MESSAGE WHERE MMSI = %s ORDER BY Timestamp DESC;""",
                        (mmsi,))
                    stat = cursor.fetchall()
                    if cursor.rowcount == 0:
                        stat[0][0] = None

                    return json.dumps({"MMSI": pos[0], "lat": float(pos[1]), "long": float(pos[2]), "IMO": stat[0][0]})

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def read_vessel_information(self, mmsi, imo=None, name=None):
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
                                      FROM (SELECT Id, MMSI, Vessel_IMO, Timestamp from AIS_MESSAGE WHERE MMSI = %s ORDER BY Timestamp DESC) t, POSITION_REPORT as pos
                                      WHERE t.Id = pos.AISMessage_Id;""", mmsi)
                    pos = cursor.fetchone()
                    if (imo is not None):
                        cursor.execute(
                            """SELECT Vessel_IMO, MMSI, Timestamp FROM AIS_MESSAGE WHERE MMSI = %s AND VESSEL_IMO = %s ORDER BY Timestamp DESC;""",
                            (mmsi, imo,))
                        imodata = cursor.fetchone()
                        if imodata is None:
                            return json.dumps({})
                    if (name is not None):
                        cursor.execute(
                            """SELECT Name FROM STATIC_DATA WHERE Name = %s ORDER BY Timestamp DESC;""",
                            (name,))
                        static = cursor.fetchone()
                        if static is None:
                            return json.dumps({})

                    return json.dumps({"MMSI": pos[0], "lat": pos[1], "long": pos[2], "IMO": imo, "Name": name})

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

    def select_all_recent_in_tile(self, tile_id):
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute("""SELECT Scale
                                      FROM MAP_VIEW
                                      WHERE MAP_VIEW.Id = %s""", tile_id)
                    rs = cursor.fetchone()[0]

                    if rs == 1:
                        cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
                                FROM (SELECT Id, MMSI, Vessel_IMO, max(Timestamp) from AIS_MESSAGE) t, POSITION_REPORT as pos, MAP_VIEW as mv
                                WHERE mv.id = pos.MapView1_Id AND t.Id = pos.AISMessage_Id;""")
                    elif rs == 2:
                        cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
                                FROM (SELECT Id, MMSI, Vessel_IMO, max(Timestamp) from AIS_MESSAGE) t, POSITION_REPORT as pos, MAP_VIEW as mv
                                WHERE mv.id = pos.MapView2_Id AND t.Id = pos.AISMessage_Id;""")
                    else:
                        cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
                                FROM (SELECT Id, MMSI, Vessel_IMO, max(Timestamp) from AIS_MESSAGE) t, POSITION_REPORT as pos, MAP_VIEW as mv
                                WHERE mv.id = pos.MapView3_Id AND t.Id = pos.AISMessage_Id;""")

                    rows = cursor.fetchall()
                    vessel = []
                    if len(rows) > 0:
                        for row in rows:
                            v = self.create_vessel_document(row)
                            vessel.append(v)

                    return json.dumps({"vessel": vessel})

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def __read_all_matching_ports__(self, port_name, optional_country=None):
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    # Most recent 5 ship positions
                    if optional_country == None:
                        cursor.execute("""SELECT Id, Name, Country, Longitude, Latitude, MapView1_Id, MapView2_Id, MapView3_Id
                                              FROM PORT
                                              WHERE PORT.Name = %s""", port_name)
                    else:
                        cursor.execute("""SELECT Id, Name, Country, Longitude, Latitude, MapView1_Id, MapView2_Id, MapView3_Id
                                          FROM PORT
                                          WHERE PORT.Name = %s AND PORT.Country = %s""", port_name, optional_country)
                    rows = cursor.fetchall()
                    ports = []
                    if len(rows) > 0:
                        for row in rows:
                            port = self.create_port_document(row)
                            ports.append(port)

                    return json.dumps({"ports": ports})

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
