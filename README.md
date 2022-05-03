# CS418_Milestone4
Monitering ships in the Northern Seas

Option A. Focus on the TMB
The safe bet, recommended: implement a fully functional TMB module.

Sample JSON documents are passed to the DAO functions, that are translated into queries specific to the DMBS of choice.
All queries for the 3 stages are implemented.
Use the DBMS of your choice: MySQL, MongoDB, or a combination of both.

## API documentation

## setup

Insert the correct values into the Config file. - connection_data.conf

RUN - `cat Milestone_4_Dump.mysql | mysql -u *** -p`
RUN - `pip install python-dateutil`
RUN - `python test_dao.py`



### Data
- Folder containing our MySQL Dump file
- #### Milestone_4_Dump.mysql - The dump file containning the mysql querries used to create the database, as well as the permament data.

### .gitignore
- keeps track of files to be ingnroed by git

### MySQL_DAO.py
- The python file containing the data access object for our mysql server.
- class MySQLConnectionManager: - handles the mysql connections using the config_data.conf file
- class MySQLCursorManager: - handles the connections used in the DAO method calls
- #### class MySQL_DAO:
- insert_ais_batch(self, json_data): - Insert a batch of AIS messages (Static Data and/or Position Reports) - Data: Array of (0,n) message documents - Return Value (JSON)
-	insert_ais_message(self, msg): - Insert an AIS message (Position Report or Static Data)	-	Message document	1/0 for Success/Fa
- delete_old_ais_messages(self): - Delete all AIS messages whose timestamp is more than 5 minutes older than current time	Current time, Timestamp	-	Number of deletions
- select_all_recent_positions(self): - Read all most recent ship positions		-	Array of ship documents3
- get_vessel_imo(self, mmsi) - Read most recent position of given MMSI	MMSI	-	Position document of the form {"MMSI": ..., "lat": ..., "long": ..., "IMO": ... }4
- get_vessel_name(self, mmsi):
- get_optional_vessel_data(self, mmsi, imo=None, name=None):
- create_vessel_document(self, vessel):
-  select_all_recent_positions(self):
- select_most_recent_from_mmsi(self, mmsi):
- read_vessel_information(self, mmsi, imo=None, name=None):
- select_most_recent_5_ship_positions(self):
-  get_tile(self, scale, long, lat):
-   select_all_recent_in_tile(self, tile_id):
-   __read_all_matching_ports__(self, port_name, optional_country=None):

### connection_data.conf
- data configuration file for mysql, set the values to be read for connections in DAO

### sample_input.json
- sample file containing json formatted list of ais_messages (position & static_data)

### test_doa.py
- python testing script for the DAO


The user/grader is given a precise description of:
the application's _existing_ modules and their functionalities
or state of completion; the general architecture; the data schema.
