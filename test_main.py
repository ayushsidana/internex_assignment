import os
import unittest

from unittest.mock import patch, Mock
from main import MySQLDatabaseConnection, RedshiftDatabaseConnection, DataExporter, DataTransformer, DataImporter, extract_transform_data

# Mocking the MySQL connection
@patch('main.mysql.connector.connect')
class TestMySQLDatabaseConnection(unittest.TestCase):

    def test_connect(self, mock_connect):
        mock_connect.return_value = Mock()
        db = MySQLDatabaseConnection('localhost', 'user', 'password', 'test_db')
        connection = db.connect()
        self.assertIsNotNone(connection)
        db.disconnect()


# Mocking the Redshift connection
@patch('main.psycopg2.connect')
class TestRedshiftDatabaseConnection(unittest.TestCase):

    def test_connect(self, mock_connect):
        mock_connect.return_value = Mock()
        db = RedshiftDatabaseConnection('localhost', 'user', 'password', 'test_db', '5439')
        connection = db.connect()
        self.assertIsNotNone(connection)
        db.disconnect()


class TestDataExporter(unittest.TestCase):
    """
    Unit tests for DataExporter class.
    """
    
    @patch('main.mysql.connector.connect')
    def test_export_data(self, mock_connect):
        """
        Test the export_data method of DataExporter class.
        """
        # Mock the cursor and its fetchall method
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [(1, 'test', 123)]  # Sample data tuple for fetchall method
        
        # Mock the connection and its cursor
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Create a MySQLDatabaseConnection instance and establish a connection
        db = MySQLDatabaseConnection('localhost', 'user', 'password', 'test_db')
        connection = db.connect()

        # Define the directory path for the exported file
        directory_path = "data_files"
        exported_file_path = os.path.join(directory_path, "exported.dat")
        
        # Mock the execute method of the cursor
        with patch.object(mock_cursor, 'execute') as mock_execute:
            mock_execute.return_value = None
            
            # Call the export_data method
            DataExporter.export_data(connection, "SELECT * FROM Data", exported_file_path)
        
        # Close the connection
        connection.close()

        # Assertions to verify the mock calls
        mock_connect.assert_called_once()
        mock_connection.cursor.assert_called_once()
        mock_execute.assert_called_once_with("SELECT * FROM Data")
        mock_cursor.fetchall.assert_called_once()


class TestDataTransformer(unittest.TestCase):

    def test_transform_data(self):
        with open("test_input.dat", "w") as f:
            f.write("1,test,123\n")
        
        DataTransformer.transform_data("test_input.dat", "test_output.dat")
        
        with open("test_output.dat", "r") as f:
            self.assertEqual(f.read().strip(), "1,123_,123")
    
    def tearDown(self):
        # Remove the created files after the test
        import os
        for filename in ["test_input.dat", "test_output.dat"]:
            if os.path.exists(filename):
                os.remove(filename)


class TestDataImporter(unittest.TestCase):

    @patch('main.psycopg2.connect')
    def test_import_data(self, mock_connect):
        # Mock the connection
        mock_connect.return_value = Mock()
        
        # Create a sample test_transformed.dat file
        with open("test_transformed.dat", "w") as f:
            f.write("sample_data")

        # Create an instance of RedshiftDatabaseConnection and connect
        connection = RedshiftDatabaseConnection('localhost', 'user', 'password', 'test_db', '5439').connect()
        
        # Mock the cursor's copy_from method
        with patch.object(connection.cursor(), 'copy_from') as mock_copy_from:
            mock_copy_from.return_value = None
            DataImporter.import_data(connection, "test_transformed.dat", "Data")
        
        # Close the connection
        connection.close()

        # Optionally, you can remove the created file after the test if needed
        import os
        os.remove("test_transformed.dat")


class TestExtractTransformData(unittest.TestCase):

    @patch('main.MySQLDatabaseConnection.connect')
    @patch('main.MySQLDatabaseConnection.disconnect')
    @patch('main.RedshiftDatabaseConnection.connect')
    @patch('main.RedshiftDatabaseConnection.disconnect')
    @patch('main.DataExporter.export_data')
    @patch('main.DataTransformer.transform_data')
    @patch('main.DataImporter.import_data')
    def test_extract_transform_data(self, mock_import_data, mock_transform_data, mock_export_data, mock_redshift_disconnect, mock_redshift_connect, mock_mysql_disconnect, mock_mysql_connect):
        
        # Mock MySQL connection and export data
        mock_mysql_connect.return_value = Mock()
        mock_export_data.return_value = None

        # Mock data transformation
        mock_transform_data.return_value = True

        # Mock Redshift connection and import data
        mock_redshift_connect.return_value = Mock()
        mock_import_data.return_value = None

        # Call the extract_transform_data function
        extract_transform_data()

        # Assertions to verify function calls
        mock_mysql_connect.assert_called_once()
        mock_export_data.assert_called_once()
        mock_transform_data.assert_called_once()
        mock_redshift_connect.assert_called_once()
        mock_import_data.assert_called_once()
        mock_mysql_disconnect.assert_called_once()
        mock_redshift_disconnect.assert_called_once()
