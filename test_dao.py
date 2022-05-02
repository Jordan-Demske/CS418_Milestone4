import unittest
import json
from MySQL_DAO import MySQL_DAO, MySQLCursorManager, MySQLConnectionManager


class TMBTest(unittest.TestCase):
    batch = """[ {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111840,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"WIND FARM BALTIC1NW\",\"VesselType\":\"Undefined\",\"Length\":60,\"Breadth\":60,\"A\":30,\"B\":30,\"C\":30,\"D\":30},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.572602,11.929218]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0,\"CoG\":298.7,\"Heading\":203},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257961000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.00316,12.809015]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0.2,\"CoG\":225.6,\"Heading\":240},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111923,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"BALTIC2 WINDFARM SW\",\"VesselType\":\"Undefined\",\"Length\":8,\"Breadth\":12,\"A\":4,\"B\":4,\"C\":4,\"D\":8},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257385000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.219403,13.127725]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":12.3,\"CoG\":96.5,\"Heading\":101},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.519373,11.47914]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":7.6,\"CoG\":294.4,\"Heading\":290} ]"""

    def test_format_ais_message_1(self):
        """
        Function `format_ais_message` will format any dictionary to include the keys needed for an AIS Message.
        """
        tmb = MySQL_DAO(True)
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
        tmb = MySQL_DAO(True)
        ais_message_data = tmb.format_ais_message({"Timestamp": "2020-11-18T00:00:00.000Z"})
        self.assertTrue(ais_message_data['Timestamp'] == "2020-11-18 00:00:00")

    def test_format_ais_message_3(self):
        """
        Function `format_ais_message` will null IMOs that are listed as 'Unknown'.
        """
        tmb = MySQL_DAO(True)
        ais_message_data = tmb.format_ais_message({"IMO": "Unknown"})
        self.assertTrue(ais_message_data['IMO'] is None)

    def test_format_position_report_1(self):
        """
        Function `format_position_report` will format any dictionary to include the keys needed for a Position Report.
        """
        tmb = MySQL_DAO(True)
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
        tmb = MySQL_DAO(True)
        position_report_data = tmb.format_position_report({'Position': {}})
        self.assertTrue(position_report_data['Position'] is None)

    def test_format_position_report_3(self):
        """
        Function `format_position_report` will count an 'Unknown value' Status attribute as null.
        """
        tmb = MySQL_DAO(True)
        position_report_data = tmb.format_position_report({'Status': 'Unknown value'})
        self.assertTrue(position_report_data['Status'] is None)

    def test_format_static_data_1(self):
        """
        Function `format_static_data` will format any dictionary to include the keys needed for a Static Data message.
        """
        tmb = MySQL_DAO(True)
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
        tmb = MySQL_DAO(True)
        static_data = tmb.format_static_data({'ETA': "2020-12-20T09:00:00.000Z"})
        self.assertTrue(static_data['ETA'] == "2020-12-20 09:00:00")

    def test_insert_ais_batch_interface_1(self):
        """
        Function `insert_ais_batch` takes a JSON string with multiple messages as input.
        """
        tmb = MySQL_DAO(True)
        inserted = tmb.insert_ais_batch(self.batch)
        self.assertTrue(type(inserted) is int and inserted >= 0)

    def test_insert_message_batch_interface_2(self):
        """
        Function `insert_message_batch` returns -1 if the input is not parsable JSON.
        """
        tmb = MySQL_DAO(True)
        inserted_count = tmb.insert_ais_batch("Not JSON")
        self.assertEqual(inserted_count, -1)

    def test_insert_message_batch_interface_3(self):
        """
        Function `insert_message_batch` returns -1 if the input is not a JSON array.
        """
        tmb = MySQL_DAO(True)
        inserted_count = tmb.insert_ais_batch("{\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}")
        self.assertEqual(inserted_count, -1)


if __name__ == '__main__':
    unittest.main()
