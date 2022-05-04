import base64
import configparser
import os
import unittest
import json
from decimal import Decimal

from MySQL_DAO import MySQL_DAO, MySQLCursorManager, MySQLConnectionManager
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime


class TMBTest(unittest.TestCase):
    batch = """[ {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111840,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"WIND FARM BALTIC1NW\",\"VesselType\":\"Undefined\",\"Length\":60,\"Breadth\":60,\"A\":30,\"B\":30,\"C\":30,\"D\":30},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.572602,11.929218]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0,\"CoG\":298.7,\"Heading\":203},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":636092297,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.00316,12.809015]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0.2,\"CoG\":225.6,\"Heading\":240},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":636092297,\"MsgType\":\"static_data\",\"IMO\":\"9534298\",\"Name\":\"Johann\",\"VesselType\":\"Undefined\",\"Length\":8,\"Breadth\":12,\"A\":4,\"B\":4,\"C\":4,\"D\":8},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257385000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.219403,13.127725]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":12.3,\"CoG\":96.5,\"Heading\":101},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.519373,11.47914]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":7.6,\"CoG\":294.4,\"Heading\":290} ]"""

    def test_format_ais_message_1(self):
        """
        Function `format_ais_message` will format any dictionary to include the keys needed for an AIS Message.
        """
        tmb = MySQL_DAO()
        ais_parameters = ['Class', 'MMSI', 'Timestamp', 'IMO']
        ais_message_data = tmb.format_ais_message({})
        includes_params = True
        for param in ais_parameters:
            if param not in ais_message_data:
                includes_params = False
        self.assertTrue(includes_params)

    def test_format_ais_message_2(self):
        """
        Function `format_ais_message` will format an ISO timestamp into one readable by MySQL.
        """
        tmb = MySQL_DAO()
        ais_message_data = tmb.format_ais_message({"Timestamp": "2020-11-18T00:00:00.000Z"})
        self.assertEqual(ais_message_data['Timestamp'], "2020-11-18 00:00:00")

    def test_format_ais_message_3(self):
        """
        Function `format_ais_message` will null IMOs that are listed as 'Unknown'.
        """
        tmb = MySQL_DAO()
        ais_message_data = tmb.format_ais_message({"IMO": "Unknown"})
        self.assertTrue(ais_message_data['IMO'] is None)

    def test_format_position_report_1(self):
        """
        Function `format_position_report` will format any dictionary to include the keys needed for a Position Report.
        """
        tmb = MySQL_DAO()
        position_report_parameters = ['RoT', 'SoG', 'CoG', 'Heading', 'Status', 'Position']
        position_report_data = tmb.format_position_report({})
        includes_params = True
        for param in position_report_parameters:
            if param not in position_report_data:
                includes_params = False
        self.assertTrue(includes_params)

    def test_format_position_report_2(self):
        """
        Function `format_position_report` will null the 'Position' attribute if it does not include point data.
        """
        tmb = MySQL_DAO()
        position_report_data = tmb.format_position_report({'Position': {}})
        self.assertTrue(position_report_data['Position'] is None)

    def test_format_position_report_3(self):
        """
        Function `format_position_report` will count an 'Unknown value' Status attribute as null.
        """
        tmb = MySQL_DAO()
        position_report_data = tmb.format_position_report({'Status': 'Unknown value'})
        self.assertTrue(position_report_data['Status'] is None)

    def test_format_static_data_1(self):
        """
        Function `format_static_data` will format any dictionary to include the keys needed for a Static Data message.
        """
        tmb = MySQL_DAO()
        static_data_parameters = ['CallSign', 'Name', 'VesselType', 'CargoType', 'Length', 'Breadth', 'Draught',
                                  'Destination', 'DestinationId', 'ETA']
        static_data = tmb.format_static_data({})
        includes_params = True
        for param in static_data_parameters:
            if param not in static_data:
                includes_params = False
        self.assertTrue(includes_params)

    def test_format_static_data_2(self):
        """
        Function `format_static_data` will format the 'ETA' attribute from ISO to a MySQL-readable format.
        """
        tmb = MySQL_DAO()
        static_data = tmb.format_static_data({'ETA': "2020-12-20T09:00:00.000Z"})
        self.assertTrue(static_data['ETA'] == "2020-12-20 09:00:00")

    def test_insert_ais_batch_interface_1(self):
        """
        Function `insert_ais_batch` takes a JSON string with multiple messages as input.
        """
        tmb = MySQL_DAO(True)
        inserted = json.loads(tmb.insert_ais_batch(self.batch))
        self.assertTrue('inserts' in inserted and type(inserted['inserts']) is int and inserted['inserts'] >= 0)

    def test_insert_ais_batch_interface_2(self):
        """
        Function `insert_ais_batch` returns -1 if the input is not parsable JSON.
        """
        tmb = MySQL_DAO(True)
        inserted_count = tmb.insert_ais_batch("Not JSON")
        self.assertEqual(inserted_count, -1)

    def test_insert_ais_batch_interface_3(self):
        """
        Function `insert_ais_batch` returns -1 if the input is not a JSON array.
        """
        tmb = MySQL_DAO(True)
        inserted_count = tmb.insert_ais_batch(
            "{\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}")
        self.assertEqual(inserted_count, -1)

    def test_insert_ais_batch_actual_1(self):
        """
        Function `insert_ais_batch` returns the number of messages inserted.
        """
        tmb = MySQL_DAO()
        inserted_count = tmb.insert_ais_batch(self.batch)
        self.assertEqual(json.loads(inserted_count)['inserts'], 7)
        tmb.delete_ais_messages()

    def test_insert_ais_batch_actual_2(self):
        """
        Function `insert_ais_batch` inserts correct data.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch("""[{\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":319904000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":319904000,\"MsgType\":\"static_data\",\"IMO\":1000021,\"Name\":\"Montkaj\",\"VesselType\":\"Yacht\",\"Length\":78,\"Breadth\":13,\"A\":30,\"B\":30,\"C\":30,\"D\":30}]""")
        ais_actual = [(1, datetime(2020, 11, 18, 0, 0), 319904000, 'Class A', None),
                      (2, datetime(2020, 11, 18, 0, 0), 319904000, 'AtoN', 1000021)]
        pos_actual = [(1, 'Under way using engine', Decimal('13.371672'), Decimal('55.218332'), Decimal('25.7'),
                       Decimal('10.8'), Decimal('94.3'), 97, None, 1, None, None)]
        stat_actual = [(2, 1000021, None, 'Montkaj', 'Yacht', None, 78, 13, None, None, None, None)]
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute(
                        """SELECT * FROM AIS_MESSAGE;""")
                    ais = cursor.fetchall()
                    cursor.execute(
                        """SELECT * FROM POSITION_REPORT;""")
                    pos = cursor.fetchall()
                    cursor.execute(
                        """SELECT * FROM STATIC_DATA;""")
                    stat = cursor.fetchall()

        except mysql.connector.Error as err:
            ais, pos, stat = [], [], []
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

        self.assertTrue((ais == ais_actual and pos == pos_actual and stat == stat_actual))

    def test_insert_ais_message_interface_1(self):
        """
        Function `insert_ais_message` exists, takes in a dictionary, and checks the type of message passed in.
        """
        tmb = MySQL_DAO(True)
        typeMsg = tmb.insert_ais_message(json.loads("{\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class "
                                                    "A\",\"MMSI\":304858000,\"MsgType\":\"position_report\","
                                                    "\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,"
                                                    "13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,"
                                                    "\"CoG\":94.3,\"Heading\":97}"))
        self.assertEqual(typeMsg, "pos")

    def test_insert_ais_message_interface_2(self):
        """
        Function `insert_ais_message` checks the type of message passed in (static data).
        """
        tmb = MySQL_DAO(True)
        typeMsg = tmb.insert_ais_message(json.loads("{\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class "
                                                    "A\",\"MMSI\":304858000,\"MsgType\":\"static_data\","
                                                    "\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,"
                                                    "13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,"
                                                    "\"CoG\":94.3,\"Heading\":97}"))
        self.assertEqual(typeMsg, "stat")

    def test_insert_ais_message_actual_1(self):
        """
        Function `insert_ais_message` returns a success message.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        result = tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":319904000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}"))
        result = json.loads(result)
        self.assertTrue('success' in result and type(result['success']) is int and result['success'] == 1)

    def test_insert_ais_message_actual_2(self):
        """
        Function `insert_ais_message` inserts a position report message into the database correctly.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        ais_actual = [(1, datetime(2020, 11, 18, 0, 0), 319904000, 'Class A', None)]
        pos_actual = [(1, 'Under way using engine', Decimal('13.371672'), Decimal('55.218332'), Decimal('25.7'),
                       Decimal('10.8'), Decimal('94.3'), 97, None, 1, None, None)]
        result = tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":319904000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}"))
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute(
                        """SELECT * FROM AIS_MESSAGE;""")
                    ais = cursor.fetchall()
                    cursor.execute(
                        """SELECT * FROM POSITION_REPORT;""")
                    pos = cursor.fetchall()
                    cursor.execute(
                        """SELECT * FROM STATIC_DATA;""")
                    stat = cursor.fetchall()

        except mysql.connector.Error as err:
            ais, pos, stat = [], [], []
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        self.assertTrue(ais == ais_actual and pos == pos_actual and stat == [])

    def test_insert_ais_message_actual_3(self):
        """
        Function `insert_ais_message` inserts a static data message into the database correctly.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        ais_actual = [(1, datetime(2020, 11, 18, 0, 0), 319904000, 'AtoN', 1000021)]
        stat_actual = [(1, 1000021, None, 'Montkaj', 'Yacht', None, 78, 13, None, None, None, None)]
        result = tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":319904000,\"MsgType\":\"static_data\",\"IMO\":1000021,\"Name\":\"Montkaj\",\"VesselType\":\"Yacht\",\"Length\":78,\"Breadth\":13,\"A\":30,\"B\":30,\"C\":30,\"D\":30}"))
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute(
                        """SELECT * FROM AIS_MESSAGE;""")
                    ais = cursor.fetchall()
                    cursor.execute(
                        """SELECT * FROM POSITION_REPORT;""")
                    pos = cursor.fetchall()
                    cursor.execute(
                        """SELECT * FROM STATIC_DATA;""")
                    stat = cursor.fetchall()

        except mysql.connector.Error as err:
            ais, pos, stat = [], [], []
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        self.assertTrue(ais == ais_actual and stat == stat_actual and pos == [])

    def test_delete_ais_messages_interface(self):
        """
        Function `delete_ais_messages` exists and returns a success message.
        """
        tmb = MySQL_DAO(True)
        result = json.loads(tmb.delete_ais_messages())
        self.assertTrue('success' in result and type(result['success']) is int and result['success'] == 1)

    def test_delete_ais_messages_actual_1(self):
        """
        Function `delete_ais_messages` returns a success message when complete.
        """
        tmb = MySQL_DAO()
        result = json.loads(tmb.delete_ais_messages())
        self.assertTrue('success' in result and type(result['success']) is int and result['success'] == 1)

    def test_delete_ais_messages_actual_2(self):
        """
        Function `delete_ais_messages` deletes all AIS Messages, leaving none left.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute(
                        """SELECT COUNT(*) FROM AIS_MESSAGE;""")
                    count = cursor.fetchone()[0]

        except mysql.connector.Error as err:
            count = 1
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        self.assertEqual(count, 0)

    def test_delete_old_ais_messages_interface(self):
        """
        Function `delete_old_ais_messages` exists and uses no parameters, returning the number of deletions.
        """
        tmb = MySQL_DAO(True)
        deletes = json.loads(tmb.delete_old_ais_messages())
        self.assertTrue('deletions' in deletes and type(deletes['deletions'] is int and deletes['deletions'] == 0))

    def test_delete_old_ais_messages_actual_1(self):
        """
        Function `delete_old_ais_messages` lists the number of records deleted.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()

        tmb.insert_ais_batch(self.batch)
        deletes = json.loads(tmb.delete_old_ais_messages())
        self.assertTrue('deletions' in deletes and type(deletes['deletions']) is int and deletes['deletions'] == 7)

    def test_delete_old_ais_messages_actual_2(self):
        """
        Function `delete_old_ais_messages` only deletes records older than 5 minutes.
        """
        tmb = MySQL_DAO()

        # All current AIS messages will be older than 5 minutes, so get rid of them
        tmb.delete_ais_messages()

        tmb.insert_ais_batch(self.batch)
        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"" + datetime.now().isoformat() + "\",\"Class\":\"AtoN\",\"MMSI\":319904000,\"MsgType\":\"static_data\",\"IMO\":1000021,\"Name\":\"Montkaj\",\"VesselType\":\"Yacht\",\"Length\":78,\"Breadth\":13,\"A\":30,\"B\":30,\"C\":30,\"D\":30}"))
        deletes = json.loads(tmb.delete_old_ais_messages())
        try:
            with MySQLConnectionManager() as con:
                with MySQLCursorManager(con) as cursor:
                    cursor.execute(
                        """SELECT COUNT(*) FROM AIS_MESSAGE;""")
                    count = cursor.fetchone()[0]

        except mysql.connector.Error as err:
            count = 0
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        self.assertTrue(
            'deletions' in deletes and type(deletes['deletions']) is int and deletes['deletions'] == 7 and count == 1)

    def test_create_vessel_document_1(self):
        """
        Function `create_vessel_document` exists and returns a vessel document based on a vessel array.
        """
        tmb = MySQL_DAO(True)
        results = tmb.create_vessel_document([333, 54.5, 12.0])
        self.assertEqual(results, {
            "MMSI": 333,
            "lat": 54.5,
            "long": 12.0,
            "Name": None,
            "IMO": None
        })

    def test_create_vessel_document_2(self):
        """
        Function `create_vessel_document` correctly fills in optional vessel data.
        """
        tmb = MySQL_DAO()
        results = tmb.create_vessel_document([636092297, 55.244508, 12.967945])
        self.assertEqual(results, {
            "MMSI": 636092297,
            "lat": 55.244508,
            "long": 12.967945,
            "Name": "Johann",
            "IMO": 9534298
        })

    def test_create_vessel_document_3(self):
        """
        Function `create_vessel_document` fails nicely when given an array that is too short
        """
        tmb = MySQL_DAO()
        results = tmb.create_vessel_document([])
        self.assertEqual(results, {
            "MMSI": None,
            "lat": None,
            "long": None,
            "Name": None,
            "IMO": None
        })

    def test_get_optional_vessel_data_1(self):
        """
        Function `get_optional_vessel_data` exists and takes an MMSI as a parameter, returning an array of two values.
        """
        tmb = MySQL_DAO(True)
        results = tmb.get_optional_vessel_data(636092297)
        self.assertEqual(results, [None, None])

    def test_get_optional_vessel_data_2(self):
        """
        Function `get_optional_vessel_data` returns null for both values if the MMSI is not found in the database.
        """
        tmb = MySQL_DAO()
        results = tmb.get_optional_vessel_data(333)
        self.assertEqual(results, ["NULL", "NULL"])

    def test_get_optional_vessel_data_3(self):
        """
        Function `get_optional_vessel_data` returns the name and IMO of a vessel based on
        transient data if such data exists.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"" + datetime.now().isoformat() + "\",\"Class\":\"AtoN\",\"MMSI\":636092297,\"MsgType\":\"static_data\",\"IMO\":1234567,\"Name\":\"Not Johann\",\"VesselType\":\"Yacht\",\"Length\":78,\"Breadth\":13,\"A\":30,\"B\":30,\"C\":30,\"D\":30}"))
        results = tmb.get_optional_vessel_data(636092297)
        self.assertEqual(results, ['Not Johann', 1234567])

    def test_get_optional_vessel_data_4(self):
        """
        Function `get_optional_vessel_data` returns the name and IMO of a vessel based on
        permanent data if no relevant transient data exists.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        results = tmb.get_optional_vessel_data(636092297)
        self.assertEqual(results, ['Johann', 9534298])

    def test_get_vessel_imo_1(self):
        """
        Function `get_vessel_imo` exists, takes in an MMSI, and returns an IMO.
        """
        tmb = MySQL_DAO(True)
        results = tmb.get_vessel_imo(636092297)
        self.assertEqual(results, 1234567)

    def test_get_vessel_imo_2(self):
        """
        Function `get_vessel_imo` returns the correct IMO.
        """
        tmb = MySQL_DAO()
        results = tmb.get_vessel_imo(636092297)
        self.assertEqual(results, 9534298)

    def test_get_vessel_imo_3(self):
        """
        Function `get_vessel_imo` returns 'NULL' on an unknown MMSI.
        """
        tmb = MySQL_DAO()
        results = tmb.get_vessel_imo(333)
        self.assertEqual(results, "NULL")

    def test_get_vessel_name_1(self):
        """
        Function `get_vessel_name` exists, takes in an MMSI, and returns a name.
        """
        tmb = MySQL_DAO(True)
        results = tmb.get_vessel_name(636092297)
        self.assertEqual(results, "Fake Name")

    def test_get_vessel_name_2(self):
        """
        Function `get_vessel_name` returns the correct name.
        """
        tmb = MySQL_DAO()
        results = tmb.get_vessel_name(636092297)
        self.assertEqual(results, "Johann")

    def test_get_vessel_name_3(self):
        """
        Function `get_vessel_name` returns 'NULL' on an unknown MMSI.
        """
        tmb = MySQL_DAO()
        results = tmb.get_vessel_name(333)
        self.assertEqual(results, "NULL")

    def test_select_all_recent_positions_interface(self):
        """
        Function `select_all_recent_positions` exists, has no parameters, and returns an array of vessel documents.
        """
        tmb = MySQL_DAO(True)
        results = json.loads(tmb.select_all_recent_positions())
        self.assertEqual(results, {"vessels": [{
            "MMSI": None,
            "lat": None,
            "long": None,
            "Name": None,
            "IMO": None
        }]})

    def test_select_all_recent_positions_actual(self):
        """
        Function `select_all_recent_positions` shows the most recent positions.
        """
        tmb = MySQL_DAO()
        results_actual = {'vessels': [
            {'MMSI': 304858000, 'lat': 55.218332, 'long': 13.371672, 'Name': 'St.Pauli', 'IMO': 8214358},
            {'MMSI': 219005465, 'lat': 54.572602, 'long': 11.929218, 'Name': 'NULL', 'IMO': 'NULL'},
            {'MMSI': 636092297, 'lat': 55.00316, 'long': 12.809015, 'Name': 'Johann', 'IMO': 9534298},
            {'MMSI': 257385000, 'lat': 55.219403, 'long': 13.127725, 'Name': 'Kegums', 'IMO': 8813972},
            {'MMSI': 376503000, 'lat': 54.519373, 'long': 11.47914, 'Name': 'Cooler Bay', 'IMO': 7818066}]}
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.select_all_recent_positions())
        self.assertEqual(results, results_actual)

    def test_select_most_recent_from_mmsi_interface(self):
        """
        Function `select_most_recent_from_mmsi` exists, takes in an MMSI, and returns a position document.
        """
        tmb = MySQL_DAO(True)
        result = json.loads(tmb.select_most_recent_from_mmsi(636092297))
        self.assertEqual(result, {"MMSI": None, "lat": None, "long": None, "IMO": None})

    def test_select_most_recent_from_mmsi_actual(self):
        """
        Function `select_most_recent_from_mmsi` creates a position document based on the most recent position data
        of a vessel with a specific MMSI.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.select_most_recent_from_mmsi(636092297))
        self.assertTrue(
            results['lat'] == 55.00316 and results['long'] == 12.809015 and results['MMSI'] == 636092297 and results[
                'IMO'] == 9534298)

    def test_read_vessel_data_interface(self):
        """
        Function `read_vessel_data` exists and returns a vessel document.
        """
        tmb = MySQL_DAO(True)
        results = json.loads(tmb.read_vessel_information(538007975))
        self.assertEqual(results, {"MMSI": None, "lat": None, "long": None, "IMO": None, "Name": None})

    def test_read_vessel_data_actual_1(self):
        """
        Function `read_vessel_data` returns information from a vessel found in transient data based on its MMSI.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.read_vessel_information(636092297))
        self.assertEqual(results,
                         {'MMSI': 636092297, 'lat': 55.00316, 'long': 12.809015, 'IMO': 9534298, 'Name': 'Johann'})

    def test_read_vessel_data_actual_2(self):
        """
        Function `read_vessel_data` returns information from a vessel found in permanent data based on its MMSI if
        there is no relevant transient data.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.read_vessel_information(538007975))
        self.assertEqual(results, {'MMSI': 538007975, 'lat': None, 'long': None, 'IMO': 9474280, 'Name': 'Leni Selmer'})

    def test_read_vessel_data_actual_3(self):
        """
        Function `read_vessel_data` returns null values in the document if the provided IMO is not found.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.read_vessel_information(538007975, 123456789))
        self.assertEqual(results, {'MMSI': None, 'lat': None, 'long': None, 'IMO': None, 'Name': None})

    def test_read_vessel_data_actual_4(self):
        """
        Function `read_vessel_data` returns null values in the document if the provided name is not found.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.read_vessel_information(538007975, name="This Name Does Not Exist"))
        self.assertEqual(results, {'MMSI': None, 'lat': None, 'long': None, 'IMO': None, 'Name': None})

    def test_read_vessel_data_actual_5(self):
        """
        Function `read_vessel_data` returns a vessel data document based on the optional IMO if it is found.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.read_vessel_information(636092297, 9534298))
        self.assertEqual(results,
                         {'MMSI': 636092297, 'lat': 55.00316, 'long': 12.809015, 'IMO': 9534298, 'Name': 'Johann'})

    def test_read_vessel_data_actual_6(self):
        """
        Function `read_vessel_data` returns a vessel data document based on the optional Name if it is found.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.read_vessel_information(636092297, name="Johann"))
        self.assertEqual(results,
                         {'MMSI': 636092297, 'lat': 55.00316, 'long': 12.809015, 'IMO': 9534298, 'Name': 'Johann'})

    def test_select_all_recent_in_tile_interface(self):
        """
        Function `select_all_recent_in_tile` exists, takes in a tile ID, and returns a list of vessels.
        """
        tmb = MySQL_DAO(True)
        results = json.loads(tmb.select_all_recent_in_tile(5037))
        self.assertEqual(results, {"vessel": []})

    def test_select_all_recent_in_tile_actual_1(self):
        """
        Function `select_all_recent_in_tile` fails nicely when given an incorrect Id.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.select_all_recent_in_tile(12345678))
        self.assertEqual(results, {"vessel": []})

    def test_select_all_recent_in_tile_actual_2(self):
        """
        Function `select_all_recent_in_tile` correctly returns a list of vessels that are in a Tile Id.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.select_all_recent_in_tile(5428))
        self.assertEqual(results, {'vessel': [
            {'MMSI': 219005465, 'lat': 54.572602, 'long': 11.929218, 'Name': 'NULL', 'IMO': 'NULL'},
            {'MMSI': 376503000, 'lat': 54.519373, 'long': 11.47914, 'Name': 'Cooler Bay', 'IMO': 7818066}]})

    def test_create_port_document_1(self):
        """
        Function `create_port_document` fails nicely when given an array that is too short.
        """
        tmb = MySQL_DAO()
        results = tmb.create_port_document([])
        self.assertEqual(results, {
            "Id": None,
            "Name": None,
            "Country": None,
            "lat": None,
            "long": None,
            "MapView1_Id": None,
            "MapView2_Id": None,
            "MapView3_Id": None
        })

    def test_create_port_document_2(self):
        """
        Function `create_port_document` correctly places values into a port document.
        """
        tmb = MySQL_DAO()
        results = tmb.create_port_document([381, "Nyborg", "Denmark", 55.298889, 10.810833, 1, 5331, 53312])
        self.assertEqual(results, {
            "Id": 381,
            "Name": "Nyborg",
            "Country": "Denmark",
            "lat": 55.298889,
            "long": 10.810833,
            "MapView1_Id": 1,
            "MapView2_Id": 5331,
            "MapView3_Id": 53312
        })

    def test_read_all_matching_ports_interface_1(self):
        """
        Function `read_all_matching_ports` exists, takes in a port name as a parameter, and returns an array of port
        documents.
        """
        tmb = MySQL_DAO(True)
        results = json.loads(tmb.read_all_matching_ports("Nyborg"))
        self.assertEqual(results, {"ports": []})

    def test_read_all_matching_ports_interface_2(self):
        """
        Function `read_all_matching_ports` allows an optional country parameter.
        """
        tmb = MySQL_DAO(True)
        results = json.loads(tmb.read_all_matching_ports("Nyborg", "Denmark"))
        self.assertEqual(results, {"ports": []})

    def test_read_all_matching_ports_actual_1(self):
        """
        Function `read_all_matching_ports` returns a correct port document based on the name.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.read_all_matching_ports("Nyborg"))
        self.assertEqual(results, {'ports': [
            {'Id': 381, 'Name': 'Nyborg', 'Country': 'Denmark', 'lat': 10.810833, 'long': 55.298889,
             'MapView1_Id': 1, 'MapView2_Id': 5331, 'MapView3_Id': 53312},
            {'Id': 4970, 'Name': 'Nyborg', 'Country': 'Denmark', 'lat': 10.790833, 'long': 55.306944,
             'MapView1_Id': 1, 'MapView2_Id': 5331, 'MapView3_Id': 53312}]})

    def test_read_all_matching_ports_actual_2(self):
        """
        Function `read_all_matching_ports` fails nicely on an unknown name.
        """
        tmb = MySQL_DAO()
        results = json.loads(tmb.read_all_matching_ports("No port here."))
        self.assertEqual(results, {'ports': []})

    def test_read_all_matching_ports_actual_3(self):
        """
        Function `read_all_matching_ports` returns a correct port document based on the country.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.read_all_matching_ports("Nyborg", "Denmark"))
        self.assertEqual(results, {'ports': [
            {'Id': 381, 'Name': 'Nyborg', 'Country': 'Denmark', 'lat': 10.810833, 'long': 55.298889,
             'MapView1_Id': 1, 'MapView2_Id': 5331, 'MapView3_Id': 53312},
            {'Id': 4970, 'Name': 'Nyborg', 'Country': 'Denmark', 'lat': 10.790833, 'long': 55.306944,
             'MapView1_Id': 1, 'MapView2_Id': 5331, 'MapView3_Id': 53312}]})

    def test_read_all_matching_ports_actual_4(self):
        """
        Function `read_all_matching_ports` fails nicely on an unknown country.
        """
        tmb = MySQL_DAO()
        results = json.loads(tmb.read_all_matching_ports("Nyborg", "Not a real country"))
        self.assertEqual(results, {'ports': []})

    def test_read_ship_pos_in_ts3_given_port_interface(self):
        """
        Function `read_ship_pos_in_ts3_given_port` exists, takes in a port name and country, and by default
        returns a list of port documents.
        """
        tmb = MySQL_DAO(True)
        results = json.loads(tmb.read_ship_pos_in_ts3_given_port("Nyborg", "Denmark"))
        self.assertEqual(results, {"ports": [{
            "Id": None,
            "Name": None,
            "Country": None,
            "lat": None,
            "long": None,
            "MapView1_Id": None,
            "MapView2_Id": None,
            "MapView3_Id": None
        }]})

    def test_read_ship_pos_in_ts3_given_port_actual_1(self):
        """
        Function `read_ship_pos_in_ts3_given_port` fails nicely if no port exists with the given parameters.
        """
        tmb = MySQL_DAO()
        results = json.loads(tmb.read_ship_pos_in_ts3_given_port("Fake Name", "Fake Port"))
        self.assertEqual(results, {"ports": [{
            "Id": None,
            "Name": None,
            "Country": None,
            "lat": None,
            "long": None,
            "MapView1_Id": None,
            "MapView2_Id": None,
            "MapView3_Id": None
        }]})

    def test_read_ship_pos_in_ts3_given_port_actual_2(self):
        """
        Function `read_ship_pos_in_ts3_given_port` returns a list of ports if multiple exist under the parameters
        given.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.read_ship_pos_in_ts3_given_port("Nyborg", "Denmark"))
        self.assertEqual(results, {'ports': [
            {'Id': 381, 'Name': 'Nyborg', 'Country': 'Denmark', 'lat': 10.810833, 'long': 55.298889, 'MapView1_Id': 1,
             'MapView2_Id': 5331, 'MapView3_Id': 53312},
            {'Id': 4970, 'Name': 'Nyborg', 'Country': 'Denmark', 'lat': 10.790833, 'long': 55.306944, 'MapView1_Id': 1,
             'MapView2_Id': 5331, 'MapView3_Id': 53312}]})

    def test_read_ship_pos_in_ts3_given_port_actual_3(self):
        """
        Function `read_ship_pos_in_ts3_given_port` returns a list of positions if only one port exists under
        the parameters given.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.read_ship_pos_in_ts3_given_port("Nysted", "Denmark"))
        self.assertEqual(results, {
            'vessel': [{'MMSI': 219005465, 'lat': 54.572602, 'long': 11.929218, 'Name': 'NULL', 'IMO': 'NULL'}]})

    def test_select_most_recent_5_ship_positions_interface(self):
        """
        Function `select_most_recent_5_ship_positions` exists, takes in an MMSI, and returns a document with
        the MMSI, a list of positions, and the IMO.
        """
        tmb = MySQL_DAO(True)
        results = json.loads(tmb.select_most_recent_5_ship_positions(636092297))
        self.assertEqual(results, {"MMSI": 636092297, "Positions": None, 'IMO': None})

    def test_select_most_recent_5_ship_positions_actual_1(self):
        """
        Function `select_most_recent_5_ship_positions` nicely fails when the MMSI is not found in position reports.
        """
        tmb = MySQL_DAO()
        results = json.loads(tmb.select_most_recent_5_ship_positions(333))
        self.assertEqual(results, {"MMSI": 333, "Positions": None, 'IMO': None})

    def test_select_most_recent_5_ship_positions_actual_2(self):
        """
        Function `select_most_recent_5_ship_positions` returns an IMO if a correct one is found for the MMSI.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.select_most_recent_5_ship_positions(636092297))
        self.assertEqual(results["IMO"], 9534298)

    def test_select_most_recent_5_ship_positions_actual_3(self):
        """
        Function `select_most_recent_5_ship_positions` returns no IMO if a correct one is not found for the MMSI.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.select_most_recent_5_ship_positions(319904000))
        self.assertEqual(results["IMO"], None)

    def test_select_most_recent_5_ship_positions_actual_4(self):
        """
        Function `select_most_recent_5_ship_positions` returns less than 5 results if less than 5 are found.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_batch(self.batch)
        results = json.loads(tmb.select_most_recent_5_ship_positions(636092297))
        self.assertEqual(results,
                         {'MMSI': 636092297, 'Positions': [{'lat': 55.00316, 'long': 12.809015}], 'IMO': 9534298})

    def test_select_most_recent_5_ship_positions_actual_5(self):
        """
        Function `select_most_recent_5_ship_positions` returns 5 results even if more are made.
        """
        tmb = MySQL_DAO()
        tmb.delete_ais_messages()
        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":319904000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.234332,12.12542]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}"))
        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"2020-11-18T00:01:00.000Z\",\"Class\":\"Class A\",\"MMSI\":319904000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.391672]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}"))
        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"2020-11-18T00:02:00.000Z\",\"Class\":\"Class A\",\"MMSI\":319904000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[56.218332,12.771672]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}"))
        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"2020-11-18T00:03:00.000Z\",\"Class\":\"Class A\",\"MMSI\":319904000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[57.218332,9.378672]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}"))
        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"2020-11-18T00:04:00.000Z\",\"Class\":\"Class A\",\"MMSI\":319904000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[58.218332,11.361672]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}"))
        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"2020-11-18T00:05:00.000Z\",\"Class\":\"Class A\",\"MMSI\":319904000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[59.218332,12.351672]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}"))
        results = json.loads(tmb.select_most_recent_5_ship_positions(319904000))
        self.assertEqual(results, {'MMSI': 319904000, 'Positions': [{'lat': 59.218332, 'long': 12.351672},
                                                                    {'lat': 58.218332, 'long': 11.361672},
                                                                    {'lat': 57.218332, 'long': 9.378672},
                                                                    {'lat': 56.218332, 'long': 12.771672},
                                                                    {'lat': 55.218332, 'long': 13.391672}],
                                   'IMO': None})

    def test_recent_ships_positions_headed_to_given_portId_interface_1(self):
        """
        Function `recent_ships_positions_headed_to_given_portId` exists, takes in a port Id, and returns an array of
        vessel documents.
        """
        tmb = MySQL_DAO(True)
        results = json.loads(tmb.recent_ships_positions_headed_to_given_portId(381))
        self.assertEqual(results, {"vessels": []})

    def test_recent_ships_positions_headed_to_given_portId_interface_2(self):
        """
        Function `recent_ships_positions_headed_to_given_portId` fails nicely when a Port Id that is not a destination
        for any vessel is supplied.
        """
        tmb = MySQL_DAO()
        results = json.loads(tmb.recent_ships_positions_headed_to_given_portId(9000))
        self.assertEqual(results, {"vessels": []})

    def test_recent_ships_positions_headed_to_given_portId_actual(self):
        """
        Function `recent_ships_positions_headed_to_given_portId` returns a list of vessels that have the port listed
        as a destination.
        """
        tmb = MySQL_DAO()

        tmb.delete_ais_messages()

        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"" + datetime.now().isoformat() + "\",\"Class\":\"AtoN\",\"DestinationId\":381,\"MMSI\":376503000,\"MsgType\":\"static_data\",\"IMO\":1234567,\"Name\":\"Not Johann\",\"VesselType\":\"Yacht\",\"Length\":78,\"Breadth\":13,\"A\":30,\"B\":30,\"C\":30,\"D\":30}"))
        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"" + datetime.now().isoformat() + "\",\"Class\":\"AtoN\",\"DestinationId\":381,\"MMSI\":219005465,\"MsgType\":\"static_data\",\"IMO\":1234567,\"Name\":\"Not Johann\",\"VesselType\":\"Yacht\",\"Length\":78,\"Breadth\":13,\"A\":30,\"B\":30,\"C\":30,\"D\":30}"))

        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"2020-11-18T00:01:00.000Z\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.391672]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}"))
        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"2020-11-18T00:02:00.000Z\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[56.218332,12.771672]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}"))

        results = json.loads(tmb.recent_ships_positions_headed_to_given_portId(381))
        self.assertEqual(results, {'vessels': [{'MMSI': 219005465, 'lat': 56.218332, 'long': 12.771672, 'IMO': 1234567},
                                               {'MMSI': 376503000, 'lat': 55.218332, 'long': 13.391672,
                                                'IMO': 1234567}]})

    def test_recent_ships_positions_headed_to_given_port_interface(self):
        """
        Function `recent_ships_positions_headed_to_given_port` exists, takes in a port name and a country, and returns
        a list of ports by default.
        """
        tmb = MySQL_DAO(True)

        results = json.loads(tmb.recent_ships_positions_headed_to_given_port("Nyborg", "Denmark"))
        self.assertEqual(results, {'ports': []})

    def test_recent_ships_positions_headed_to_given_port_actual_1(self):
        """
        Function `recent_ships_positions_headed_to_given_port` returns the positions of recent ships if a unique
        port is found using the parameters.
        """
        tmb = MySQL_DAO()

        tmb.delete_ais_messages()

        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"" + datetime.now().isoformat() + "\",\"Class\":\"AtoN\",\"DestinationId\":4384,\"MMSI\":376503000,\"MsgType\":\"static_data\",\"IMO\":1234567,\"Name\":\"Not Johann\",\"VesselType\":\"Yacht\",\"Length\":78,\"Breadth\":13,\"A\":30,\"B\":30,\"C\":30,\"D\":30}"))
        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"" + datetime.now().isoformat() + "\",\"Class\":\"AtoN\",\"DestinationId\":4384,\"MMSI\":219005465,\"MsgType\":\"static_data\",\"IMO\":1234567,\"Name\":\"Not Johann\",\"VesselType\":\"Yacht\",\"Length\":78,\"Breadth\":13,\"A\":30,\"B\":30,\"C\":30,\"D\":30}"))

        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"2020-11-18T00:01:00.000Z\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.391672]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}"))
        tmb.insert_ais_message(json.loads(
            "{\"Timestamp\":\"2020-11-18T00:02:00.000Z\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[56.218332,12.771672]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}"))

        results = json.loads(tmb.recent_ships_positions_headed_to_given_port("Nysted", "Denmark"))
        self.assertEqual(results, {
            'vessels': [{'MMSI': 219005465, 'lat': 56.218332, 'long': 12.771672, 'Name': 'Not Johann', 'IMO': 1234567},
                        {'MMSI': 376503000, 'lat': 55.218332, 'long': 13.391672, 'Name': 'Not Johann',
                         'IMO': 1234567}]})

    def test_recent_ships_positions_headed_to_given_port_actual_2(self):
        """
        Function `recent_ships_positions_headed_to_given_port` returns the a list of ports if no specific port was found
        with the criteria given.
        """
        tmb = MySQL_DAO()

        results = json.loads(tmb.recent_ships_positions_headed_to_given_port("Nyborg", "Denmark"))
        self.assertEqual(results, {'ports': [
            {'Id': 381, 'Name': 'Nyborg', 'Country': 'Denmark', 'lat': 10.810833, 'long': 55.298889, 'MapView1_Id': 1,
             'MapView2_Id': 5331, 'MapView3_Id': 53312},
            {'Id': 4970, 'Name': 'Nyborg', 'Country': 'Denmark', 'lat': 10.790833, 'long': 55.306944, 'MapView1_Id': 1,
             'MapView2_Id': 5331, 'MapView3_Id': 53312}]})

    def test_recent_ships_positions_headed_to_given_port_actual_3(self):
        """
        Function `recent_ships_positions_headed_to_given_port` nicely fails if no port is found.
        """
        tmb = MySQL_DAO()

        results = json.loads(tmb.recent_ships_positions_headed_to_given_port("Nothing", "Empty"))
        self.assertEqual(results, {'ports': []})

    def test_recent_ships_positions_headed_to_given_port_actual_4(self):
        """
        Function `recent_ships_positions_headed_to_given_port` nicely fails if a specific port is found, but no vessels.
        """
        tmb = MySQL_DAO()

        results = json.loads(tmb.recent_ships_positions_headed_to_given_port("Dansk", "Denmark"))
        self.assertEqual(results, {'vessels': []})

    def test_create_tile_document_1(self):
        """
        Function `create_tile_document` fails nicely when given an array that is too short.
        """
        tmb = MySQL_DAO()
        results = tmb.create_tile_document([])
        self.assertEqual(results, {'Id': None, 'Name': None, 'LongitudeW': None, 'LatitudeS': None, 'LongitudeE': None,
                                   'LatitudeN': None, 'Scale': None, 'RasterFile': None, 'ImageWidth': None,
                                   'ImageHeight': None, 'ActualLongitudeW': None, 'ActualLatitudeS': None,
                                   'ActualLongitudeE': None, 'ActualLatitudeN': None, 'ContainerMapView_Id': None})

    def test_create_tile_document_2(self):
        """
        Function `create_tile_document` correctly places values into a tile document.
        """
        tmb = MySQL_DAO()
        results = tmb.create_tile_document(
            [1, None, Decimal('7.000000'), Decimal('54.500000'), Decimal('13.000000'), Decimal('57.500000'), '1',
             'ROOT.png', 2000, 2000, Decimal('7.000000'), Decimal('54.316140'), Decimal('13.000000'),
             Decimal('57.669343'), None])
        self.assertEqual(results, {'Id': 1, 'Name': None, 'LongitudeW': 7.0, 'LatitudeS': 54.5, 'LongitudeE': 13.0,
                                   'LatitudeN': 57.5, 'Scale': '1', 'RasterFile': 'ROOT.png', 'ImageWidth': 2000,
                                   'ImageHeight': 2000, 'ActualLongitudeW': 7.0, 'ActualLatitudeS': 54.31614,
                                   'ActualLongitudeE': 13.0, 'ActualLatitudeN': 57.669343, 'ContainerMapView_Id': None})

    def test_given_tile_find_contained_tiles_interface(self):
        """
        Function `given_tile_find_contained_tiles` exists, takes in a tile id, and returns a list of tiles.
        """
        tmb = MySQL_DAO(True)

        results = json.loads(tmb.given_tile_find_contained_tiles(5036))
        self.assertEqual(results, {'tiles': []})

    def test_given_tile_find_contained_tiles_actual_1(self):
        """
        Function `given_tile_find_contained_tiles` nicely fails when given a wrong tile Id.
        """
        tmb = MySQL_DAO()

        results = json.loads(tmb.given_tile_find_contained_tiles(9999999))
        self.assertEqual(results, {'tiles': []})

    def test_given_tile_find_contained_tiles_actual_2(self):
        """
        Function `given_tile_find_contained_tiles` correctly finds the four tiles that the tile Id passed in contains
        and lists them in an array.
        """
        tmb = MySQL_DAO()

        results = json.loads(tmb.given_tile_find_contained_tiles(5036))
        self.assertEqual(results, {'tiles': [
            {'Id': 50361, 'Name': '38F71', 'LongitudeW': 7.0, 'LatitudeS': 54.75, 'LongitudeE': 7.5, 'LatitudeN': 55.0,
             'Scale': '3', 'RasterFile': '38F71.png', 'ImageWidth': 2000, 'ImageHeight': 2000, 'ActualLongitudeW': 7.0,
             'ActualLatitudeS': 54.731097, 'ActualLongitudeE': 7.5, 'ActualLatitudeN': 55.018777,
             'ContainerMapView_Id': 5036},
            {'Id': 50362, 'Name': '38F72', 'LongitudeW': 7.5, 'LatitudeS': 54.75, 'LongitudeE': 8.0, 'LatitudeN': 55.0,
             'Scale': '3', 'RasterFile': '38F72.png', 'ImageWidth': 2000, 'ImageHeight': 2000, 'ActualLongitudeW': 7.5,
             'ActualLatitudeS': 54.731097, 'ActualLongitudeE': 8.0, 'ActualLatitudeN': 55.018777,
             'ContainerMapView_Id': 5036},
            {'Id': 50363, 'Name': '38F73', 'LongitudeW': 7.0, 'LatitudeS': 54.5, 'LongitudeE': 7.5, 'LatitudeN': 54.75,
             'Scale': '3', 'RasterFile': '38F73.png', 'ImageWidth': 2000, 'ImageHeight': 2000, 'ActualLongitudeW': 7.0,
             'ActualLatitudeS': 54.480204, 'ActualLongitudeE': 7.5, 'ActualLatitudeN': 54.769665,
             'ContainerMapView_Id': 5036},
            {'Id': 50364, 'Name': '38F74', 'LongitudeW': 7.5, 'LatitudeS': 54.5, 'LongitudeE': 8.0, 'LatitudeN': 54.75,
             'Scale': '3', 'RasterFile': '38F74.png', 'ImageWidth': 2000, 'ImageHeight': 2000, 'ActualLongitudeW': 7.5,
             'ActualLatitudeS': 54.480204, 'ActualLongitudeE': 8.0, 'ActualLatitudeN': 54.769665,
             'ContainerMapView_Id': 5036}]})

    def test_given_tile_id_get_tile_interface(self):
        """
        Function `given_tile_id_get_tile` exists and takes in a tile Id.
        """
        tmb = MySQL_DAO(True)

        results = tmb.given_tile_id_get_tile(50361)
        self.assertEqual(results, 50361)

    def test_given_tile_id_get_tile_actual_1(self):
        """
        Function `given_tile_id_get_tile` returns -1 when an unknown map tile id is passed.
        """
        tmb = MySQL_DAO()

        results = tmb.given_tile_id_get_tile(8675309)
        self.assertEqual(results, -1)

    def test_given_tile_id_get_tile_actual_2(self):
        """
        Function `given_tile_id_get_tile` returns the file of the map tile id.
        """
        tmb = MySQL_DAO()

        results = tmb.given_tile_id_get_tile(50361)

        path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'denmark_tiles', '38F71.png'))
        with open(path, 'rb') as f:
            file_data = base64.b64encode(f.read())
            self.assertEqual(results, file_data)

    def test_get_tile_1(self):
        """
        Function `get_tile` returns the calculated boundaries of a tile at scale 1.
        """
        tmb = MySQL_DAO()

        results = tmb.get_tile(1, 11.47914, 54.519373)

        self.assertEqual(results, {'south': 54.5, 'north': 57.5, 'west': 7.0, 'east': 13.0})

    def test_get_tile_2(self):
        """
        Function `get_tile` returns the calculated boundaries of a tile at scale 2.
        """
        tmb = MySQL_DAO()

        results = tmb.get_tile(2, 11.47914, 54.519373)

        self.assertEqual(results, {'south': 54.5, 'north': 55.0, 'west': 11, 'east': 12})

    def test_get_tile_3(self):
        """
        Function `get_tile` returns the calculated boundaries of a tile at scale 2.
        """
        tmb = MySQL_DAO()

        results = tmb.get_tile(3, 11.47914, 54.519373)

        self.assertEqual(results, {'south': 54.5, 'north': 54.75, 'west': 11.0, 'east': 11.5})


if __name__ == '__main__':
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'Milestone_4_Dump.mysql'))
    config_file = 'connection_data.conf'
    config = configparser.ConfigParser()
    config.read(config_file)

    os.system('cat ' + path + ' | mysql -u ' + config['SQL']['user'] + ' --password=' + config['SQL']['password'])

    unittest.main(verbosity=2)
