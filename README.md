# CS418 Milestone 4 Option A Group 5
**To start, you must have:**
A Python version higher than 3
dateutil *(If you do not have it, run `pip install python-dateutil`, assuming you have pip. Otherwise, use your package manager of choice)*
The other packages, such as mysql, json, os, math, and base64, should come included in Python. Otherwise, install them.

# Running the Program
First, make sure that you put in your MySQL user credentials in `connection_data.conf`
The database name should stay as 'milestone4'

To run, make sure you are in the base project directory, and run:
`python test_dao.py`
This will create the database, populate it, and run our tests.
The tests display a short snippet saying what they are for.
Every part of this project is complete, so we have unit tests and implementation tests for every query needed by our DAO application.

Our program has been untested on Linux, so we recommend testing be done on Windows.

# Documentation
To view our documentation, either look at the code, or use the HTML document created by Sphinx for our project.
It is located in `CS418_Milestone4\docs\_build\html\index.html`