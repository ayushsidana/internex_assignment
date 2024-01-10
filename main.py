import mysql.connector
import psycopg2
import os
import logging
from settings import DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME, RED_SHIFT_HOST, RED_SHIFT_USER, RED_SHIFT_PASSWORD, RED_SHIFT_DBNAME, RED_SHIFT_PORT

logging.basicConfig(level=logging.INFO)


class DatabaseConnection:
    """
    Base class for establishing a database connection.
    """
    def __init__(self, host, user, password, database=None, port=None):
        """
        Initialize the database connection parameters.
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def connect(self):
        """
        Establish a database connection.
        """
        pass

    def disconnect(self):
        """
        Close the database connection.
        """
        if self.connection:
            self.connection.close()
            logging.info(f"Disconnected from {self.host}")


class MySQLDatabaseConnection(DatabaseConnection):
    """
    Class to establish a connection with MySQL database.
    """
    def connect(self):
        """
        Establish a connection with MySQL database.
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            logging.info(f"Connected to MySQL database at {self.host}")
            return self.connection
        except Exception as e:
            logging.error(f"Error connecting to MySQL: {e}")
            raise e


class RedshiftDatabaseConnection(DatabaseConnection):
    """
    Class to establish a connection with Redshift database.
    """
    def connect(self):
        """
        Establish a connection with Redshift database.
        """
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                dbname=self.database,
                port=self.port
            )
            logging.info(f"Connected to Redshift database at {self.host}")
            return self.connection
        except Exception as e:
            logging.error(f"Error connecting to Redshift: {e}")
            raise e


class DataExporter:
    """
    Class to export data from a database to a file.
    """
    @staticmethod
    def export_data(connection, query, filename):
        """
        Export data from the database to a file.
        """
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            
            if not os.path.exists(os.path.dirname(filename)):
                os.makedirs(os.path.dirname(filename))
            
            with open(filename, "w") as file:
                for row in cursor.fetchall():
                    file.write(','.join(map(str, row)) + '\n')
            
            cursor.close()
            logging.info(f"Exported data to {filename}")
        except Exception as e:
            logging.error(f"Error exporting data: {e}")
            raise e


class DataTransformer:
    """
    Class to transform data from one format to another.
    """
    @staticmethod
    def transform_data(input_file, output_file):
        """
        Transform data from input file and save it to output file.
        """
        try:
            with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
                for line in infile:
                    columns = line.strip().split(',')
                    columns[1] = columns[2].upper() + "_"
                    outfile.write(','.join(columns) + '\n')
            logging.info(f"Transformed data and saved to {output_file}")
        except Exception as e:
            logging.error(f"Error transforming data: {e}")
            raise e


class DataImporter:
    """
    Class to import data from a file to a database.
    """
    @staticmethod
    def import_data(connection, filename, table_name):
        """
        Import data from a file to the database table.
        """
        try:
            cursor = connection.cursor()
            
            with open(filename, "r") as file:
                next(file)
                cursor.copy_from(file, table_name, sep=',')
            
            connection.commit()
            cursor.close()
            logging.info(f"Imported data into {table_name}")
        except Exception as e:
            logging.error(f"Error importing data: {e}")
            raise e


def extract_transform_data():
    """
    This function performs the following steps:
    1. Connects to a MySQL database and exports data from a table where the 'Flag' column is set to TRUE.
    2. Transforms the exported data.
    3. Connects to a Redshift database and imports the transformed data into a table named 'Data'.
    """
    try:
        # MySQL Connection and Export
        mysql_db = MySQLDatabaseConnection(DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME)
        mysql_conn = mysql_db.connect()
        query = "SELECT * FROM Data WHERE Flag = TRUE"
        exported_file_path = "data_files/exported.dat"
        DataExporter.export_data(mysql_conn, query, exported_file_path)
        mysql_db.disconnect()

        # Transformation
        transformed_file_path = "data_files/transformed.dat"
        DataTransformer.transform_data(exported_file_path, transformed_file_path)

        # Redshift Connection and Import
        redshift_db = RedshiftDatabaseConnection(RED_SHIFT_HOST, RED_SHIFT_USER, RED_SHIFT_PASSWORD, RED_SHIFT_DBNAME, RED_SHIFT_PORT)
        redshift_conn = redshift_db.connect()
        DataImporter.import_data(redshift_conn, transformed_file_path, 'Data')
        redshift_db.disconnect()

    except Exception as e:
        logging.error(f"Error: {e}")
