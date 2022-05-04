import base64
import json
import math
import os
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
        if "Position" in msg and msg["Position"] is not None:
            for parameter in self.position_parameters:
                if msg['Position'] != {}:
                    if type(msg['Position']) is not dict:
                        msg['Position'] = None
                    elif parameter not in msg['Position']:
                        msg['Position'] = None
                else:
                    msg['Position'] = None
        else:
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
            return -1
        if not isinstance(data, list):
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
                    if cursor.rowcount == 0:
                        return json.dumps({"success": 0})

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

    def delete_ais_messages(self):
        if self.is_stub:
            return json.dumps({"success": 1})
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute(
                        """DELETE FROM POSITION_REPORT;""")
                    cursor.execute(
                        """DELETE FROM STATIC_DATA;""")
                    cursor.execute(
                        """DELETE FROM AIS_MESSAGE;""")
                    cursor.execute("""ALTER TABLE AIS_MESSAGE AUTO_INCREMENT = 1;""")
                    con.commit()
                    return json.dumps({"success": 1})

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def delete_old_ais_messages(self):
        if self.is_stub:
            return json.dumps({"deletions": 0})
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
        if self.is_stub:
            return 1234567
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
        if self.is_stub:
            return "Fake Name"
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
        if self.is_stub:
            return [None, None]
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    data = ["NULL", "NULL"]
                    cursor.execute(
                        """SELECT Name, AISIMO FROM STATIC_DATA, AIS_MESSAGE WHERE AISMessage_Id = Id AND MMSI = %s ORDER BY Timestamp ASC;""",
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
        if len(vessel) < 3:
            return {
                "MMSI": None,
                "lat": None,
                "long": None,
                "Name": None,
                "IMO": None
            }
        if self.is_stub:
            return {
                "MMSI": vessel[0],
                "lat": float(vessel[1]),
                "long": float(vessel[2]),
                "Name": None,
                "IMO": None
            }
        optional_data = self.get_optional_vessel_data(vessel[0])
        return {
            "MMSI": vessel[0],
            "lat": float(vessel[1]),
            "long": float(vessel[2]),
            "Name": optional_data[0],
            "IMO": optional_data[1]
        }

    def select_all_recent_positions(self):
        if self.is_stub:
            return json.dumps({"vessels": [{
                "MMSI": None,
                "lat": None,
                "long": None,
                "Name": None,
                "IMO": None
            }]})
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
            return json.dumps({"MMSI": None, "lat": None, "long": None, "IMO": None})
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
                        """SELECT Vessel_IMO, MMSI, Timestamp FROM AIS_MESSAGE WHERE MMSI = %s AND Vessel_IMO IS NOT NULL ORDER BY Timestamp DESC;""",
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
        if self.is_stub:
            return json.dumps({"MMSI": None, "lat": None, "long": None, "IMO": None, "Name": None})
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
                                      FROM (SELECT Id, MMSI, Vessel_IMO, Timestamp from AIS_MESSAGE WHERE MMSI = %s ORDER BY Timestamp DESC) t, POSITION_REPORT as pos
                                      WHERE t.Id = pos.AISMessage_Id;""", (mmsi,))
                    pos = cursor.fetchone()
                    if cursor.rowcount == 0:
                        pos = [None, None, None]
                    else:
                        lat = pos[1]
                        long = pos[2]
                        pos = [None, None, None]
                        pos[1] = float(lat)
                        pos[2] = float(long)

                    if imo is not None:
                        cursor.execute(
                            """SELECT Vessel_IMO, MMSI, Timestamp FROM AIS_MESSAGE WHERE MMSI = %s AND VESSEL_IMO = %s ORDER BY Timestamp DESC;""",
                            (mmsi, imo,))
                        imodata = cursor.fetchone()
                        if cursor.rowcount == 0:
                            return json.dumps({"MMSI": None, "lat": None, "long": None, "IMO": None, "Name": None})
                        else:
                            imodata = imodata[0]
                    else:
                        cursor.execute(
                            """SELECT Vessel_IMO, MMSI, Timestamp FROM AIS_MESSAGE WHERE MMSI = %s AND Vessel_IMO IS NOT NULL ORDER BY Timestamp DESC;""",
                            (mmsi,))
                        imodata = cursor.fetchone()
                        if cursor.rowcount == 0:
                            cursor.execute(
                                """SELECT IMO FROM VESSEL WHERE MMSI = %s;""",
                                (mmsi,))
                            imodata = cursor.fetchone()
                            if cursor.rowcount == 0:
                                imodata = None
                            else:
                                imodata = imodata[0]
                        else:
                            imodata = imodata[0]

                    if name is not None:
                        cursor.execute(
                            """SELECT Name FROM STATIC_DATA, AIS_MESSAGE WHERE Name = %s AND MMSI = %s AND AISMessage_Id = Id ORDER BY Timestamp DESC;""",
                            (name, mmsi))
                        namedata = cursor.fetchone()
                        if cursor.rowcount == 0:
                            return json.dumps({"MMSI": None, "lat": None, "long": None, "IMO": None, "Name": None})
                        else:
                            namedata = namedata[0]
                    else:
                        cursor.execute(
                            """SELECT Name FROM STATIC_DATA, AIS_MESSAGE WHERE MMSI = %s AND AISMessage_Id = Id ORDER BY Timestamp DESC;""",
                            (mmsi,))
                        namedata = cursor.fetchone()
                        if cursor.rowcount == 0:
                            cursor.execute(
                                """SELECT Name FROM VESSEL WHERE MMSI = %s;""",
                                (mmsi,))
                            namedata = cursor.fetchone()
                            if cursor.rowcount == 0:
                                namedata = None
                            else:
                                namedata = namedata[0]
                        else:
                            namedata = namedata[0]

                    return json.dumps({"MMSI": mmsi, "lat": pos[1], "long": pos[2], "IMO": imodata, "Name": namedata})

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def select_most_recent_5_ship_positions(self, mmsi):
        if self.is_stub:
            return json.dumps({"MMSI": mmsi, "Positions": None, 'IMO': None})
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
                                      FROM (SELECT Id, MMSI, Vessel_IMO, Timestamp FROM AIS_MESSAGE WHERE MMSI = %s ORDER BY Timestamp DESC LIMIT 5) t, POSITION_REPORT as pos
                                      WHERE t.Id = pos.AISMessage_Id;""", (mmsi,))
                    pos = cursor.fetchall()
                    if cursor.rowcount == 0:
                        return json.dumps({"MMSI": mmsi, "Positions": None, 'IMO': None})

                    positions = []
                    for position in pos:
                        temp_pos = {'lat': float(position[1]), 'long': float(position[2])}
                        positions.append(temp_pos)

                    cursor.execute(
                        """SELECT AISIMO FROM STATIC_DATA, AIS_MESSAGE WHERE AISMessage_Id = Id AND MMSI = %s ORDER BY Timestamp ASC;""",
                        (mmsi,))
                    rows = cursor.fetchall()
                    imo = None
                    if cursor.rowcount > 0:
                        for row in rows:
                            if row[0] is not None:
                                imo = row[0]

                    return json.dumps({"MMSI": mmsi, "Positions": positions, 'IMO': imo})

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def recent_ships_positions_headed_to_given_portId(self, port_id):
        if self.is_stub:
            return json.dumps({"vessels": []})
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute("""SELECT t.MMSI, pos.Latitude, pos.Longitude
                                      FROM (SELECT Id, MMSI, MAX(Timestamp) as LatestTime FROM AIS_MESSAGE WHERE Vessel_IMO IS NULL GROUP BY MMSI) t, POSITION_REPORT as pos, STATIC_DATA as sd
                                      WHERE t.Id = pos.AISMessage_Id AND pos.LastStaticData_Id = sd.AISMessage_Id AND sd.DestinationPort_Id = %s ORDER BY t.LatestTime DESC;""",
                                   (port_id,))
                    rows = cursor.fetchall()
                    vessels = []
                    if cursor.rowcount > 0:
                        for row in rows:
                            vessel = self.create_vessel_document(row)
                            del vessel['Name']
                            vessels.append(vessel)
                    else:
                        json.dumps({"vessels": []})

                    return json.dumps({"vessels": vessels})

        # return array of position documents

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def create_port_document(self, port):
        if len(port) < 8:
            return {
                "Id": None,
                "Name": None,
                "Country": None,
                "lat": None,
                "long": None,
                "MapView1_Id": None,
                "MapView2_Id": None,
                "MapView3_Id": None
            }
        return {
            "Id": port[0],
            "Name": port[1],
            "Country": port[2],
            "lat": float(port[3]),
            "long": float(port[4]),
            "MapView1_Id": port[5],
            "MapView2_Id": port[6],
            "MapView3_Id": port[7]
        }

    def recent_ships_positions_headed_to_given_port(self, port_name, country):
        if self.is_stub:
            return json.dumps({"ports": []})
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute("""SELECT PORT.Id FROM PORT WHERE PORT.Name = %s AND PORT.Country = %s;""",
                                   (port_name, country))
                    id = cursor.fetchall()
                    if cursor.rowcount == 0:
                        return json.dumps({"ports": []})
                    elif cursor.rowcount > 1:
                        return self.read_all_matching_ports(port_name, country)
                    else:
                        cursor.execute(
                            """SELECT t.MMSI, pos.Latitude, pos.Longitude FROM (SELECT Id, MMSI, MAX(Timestamp) as 
                            LatestTime from AIS_MESSAGE WHERE Vessel_IMO IS NULL GROUP BY MMSI) t, POSITION_REPORT as 
                            pos, PORT as port, STATIC_DATA as sd WHERE t.Id = pos.AISMessage_Id AND pos.LastStaticData_Id 
                            = sd.AISMessage_Id AND sd.DestinationPort_Id = port.Id AND port.Name = %s AND 
                            port.Country = %s ORDER BY t.LatestTime DESC;""",
                            (port_name, country))
                        rows = cursor.fetchall()
                        vessels = []
                        if cursor.rowcount > 0:
                            for row in rows:
                                vessel = self.create_vessel_document(row)
                                vessels.append(vessel)
                            return json.dumps({"vessels": vessels})
                        else:
                            return json.dumps({"vessels": []})

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
        if self.is_stub:
            return json.dumps({"vessel": []})
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute("""SELECT Scale
                                      FROM MAP_VIEW
                                      WHERE MAP_VIEW.Id = %s""", (tile_id,))
                    if cursor.rowcount == 0:
                        return json.dumps({"vessel": []})
                    rs = cursor.fetchone()[0]
                    statement = """SELECT t.MMSI, pos.Latitude, pos.Longitude, t.Vessel_IMO
                                                    FROM (SELECT Id, MMSI, Vessel_IMO, max(Timestamp) max from AIS_MESSAGE WHERE Vessel_IMO IS NULL GROUP BY MMSI) t, POSITION_REPORT as pos
                                                    WHERE pos.MapView""" + str(
                        rs) + """_Id = %s AND t.Id = pos.AISMessage_Id;"""
                    cursor.execute(statement, (tile_id,))
                    rows = cursor.fetchall()
                    vessel = []
                    if cursor.rowcount > 0:
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

    def read_all_matching_ports(self, port_name, country=None):
        if self.is_stub:
            return json.dumps({"ports": []})
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    if country is None:
                        cursor.execute("""SELECT Id, Name, Country, Longitude, Latitude, MapView1_Id, MapView2_Id, MapView3_Id
                                              FROM PORT
                                              WHERE PORT.Name = %s""", (port_name,))
                    else:
                        cursor.execute("""SELECT Id, Name, Country, Longitude, Latitude, MapView1_Id, MapView2_Id, MapView3_Id
                                          FROM PORT
                                          WHERE PORT.Name = %s AND PORT.Country = %s""", (port_name, country))
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

    def read_ship_pos_in_ts3_given_port(self, port_name, country):
        if self.is_stub:
            return json.dumps({"ports": [{
                "Id": None,
                "Name": None,
                "Country": None,
                "lat": None,
                "long": None,
                "MapView1_Id": None,
                "MapView2_Id": None,
                "MapView3_Id": None
            }]})
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute("""SELECT MAP_VIEW.id
                                      FROM MAP_VIEW, PORT
                                      WHERE PORT.MapView3_Id = MAP_VIEW.Id AND PORT.Name = %s AND PORT.Country = %s;""",
                                   (port_name, country))
                    id = cursor.fetchall()
                    if cursor.rowcount == 0:
                        return json.dumps({"ports": [{
                            "Id": None,
                            "Name": None,
                            "Country": None,
                            "lat": None,
                            "long": None,
                            "MapView1_Id": None,
                            "MapView2_Id": None,
                            "MapView3_Id": None
                        }]})
                    elif cursor.rowcount > 1:
                        return self.read_all_matching_ports(port_name, country)
                    else:
                        return self.select_all_recent_in_tile(id[0][0])

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def create_tile_document(self, tile):
        if len(tile) < 15:
            return {
                "Id": None,
                "Name": None,
                "LongitudeW": None,
                "LatitudeS": None,
                "LongitudeE": None,
                "LatitudeN": None,
                "Scale": None,
                "RasterFile": None,
                "ImageWidth": None,
                "ImageHeight": None,
                "ActualLongitudeW": None,
                "ActualLatitudeS": None,
                "ActualLongitudeE": None,
                "ActualLatitudeN": None,
                "ContainerMapView_Id": None
            }
        return {
            "Id": tile[0],
            "Name": tile[1],
            "LongitudeW": float(tile[2]),
            "LatitudeS": float(tile[3]),
            "LongitudeE": float(tile[4]),
            "LatitudeN": float(tile[5]),
            "Scale": tile[6],
            "RasterFile": tile[7],
            "ImageWidth": tile[8],
            "ImageHeight": tile[9],
            "ActualLongitudeW": float(tile[10]),
            "ActualLatitudeS": float(tile[11]),
            "ActualLongitudeE": float(tile[12]),
            "ActualLatitudeN": float(tile[13]),
            "ContainerMapView_Id": tile[14]
        }

    def given_tile_find_contained_tiles(self, map_tile_id):
        if self.is_stub:
            return json.dumps({"tiles": []})
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute("""SELECT map3.*
                                      FROM MAP_VIEW as map3, MAP_VIEW as map2
                                      WHERE map2.Id= %s AND map3.ContainerMapView_Id=map2.Id;""", (map_tile_id,))
                    rows = cursor.fetchall()
                    tiles = []
                    if cursor.rowcount > 0:
                        for row in rows:
                            tile = self.create_tile_document(row)
                            tiles.append(tile)
                    return json.dumps({"tiles": tiles})

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def given_tile_id_get_tile(self, map_tile_id):
        if self.is_stub:
            return map_tile_id
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute("""SELECT MAP_VIEW.RasterFile
                                       FROM MAP_VIEW
                                       WHERE MAP_VIEW.Id = %s""", (map_tile_id,))
                    if cursor.rowcount == 0:
                        return -1
                    tile_image = cursor.fetchone()[0]
                    path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'denmark_tiles', tile_image))
                    with open(path, 'rb') as f:
                        file_data = base64.b64encode(f.read())
                        return file_data

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
