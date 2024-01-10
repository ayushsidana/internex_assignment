# internex_assignment

Database Data Transfer and Transformation Utility
This utility provides a framework to connect to MySQL and Redshift databases, export data from MySQL, transform it, and import it into Redshift.

Features:
1. Connect to MySQL Database: Establish a connection to a MySQL database.
2. Connect to Redshift Database: Establish a connection to a Redshift database.
3. Export Data from MySQL: Fetch data from a MySQL table where a specific condition is met and export it to a file.
4. Transform Data: Transform the exported data by converting a specific column value to uppercase and appending an underscore.
5. Import Data into Redshift: Import the transformed data into a Redshift table.

Setup:
Prerequisites:
Python 3.x installed
Necessary packages installed (mysql.connector, psycopg2)
Steps:
Clone the repository to your local machine:

bash
Copy code
git clone https://github.com/your-repository-link.git
Navigate to the project directory:

bash
Copy code
cd your-project-directory
Install the required Python packages:

bash
Copy code
pip install mysql.connector psycopg2-binary
Usage:
Run the Main Script:

Modify the main.py script to include your specific database credentials and configurations. Execute the script to perform the data transfer and transformation.

bash
Copy code
python main.py
Unit Testing:

To ensure the functionalities work correctly, unit tests have been set up using pytest. Run the tests using the following command:

bash
Copy code
pytest test_main.py
