"""
Integration tests for batch_loader.py

Note: These tests interact with actual GCP resources and NOAA APIs,
so they should be run in a test environment with proper credentials.
"""

import os
import sys
import unittest
import tempfile
from unittest.mock import patch, MagicMock

# Add the parent directory to sys.path to allow importing from src
import sys
from pathlib import Path

parent_dir = str(Path(__file__).resolve().parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from src.batch_loader import (
    load_env_vars,
    init_gcs_client,
    init_iceberg_catalog,
    download_and_upload_to_gcs,
    process_stations_data,
    process_observations_data,
    main
)


class TestBatchLoaderIntegration(unittest.TestCase):
    """Integration tests for batch_loader.py"""

    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        # Load environment variables
        load_env_vars()
        
        # Set test environment variables if not already set
        if 'INTEGRATION_TEST' not in os.environ:
            # Skip actual API calls and uploads for normal test runs
            os.environ['INTEGRATION_TEST'] = 'False'
        
        # Create a temporary test directory
        cls.test_dir = tempfile.TemporaryDirectory()

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment"""
        # Remove the temporary directory
        cls.test_dir.cleanup()

    def setUp(self):
        """Set up each test"""
        # Skip tests if not in integration test mode
        if os.environ.get('INTEGRATION_TEST', 'False').lower() != 'true':
            self.skipTest("Skipping integration test in non-integration mode")

    @patch('src.batch_loader.init_gcs_client')
    @patch('src.batch_loader.init_iceberg_catalog')
    def test_process_stations_data_integration(self, mock_init_catalog, mock_init_gcs):
        """Test processing stations data with actual API calls"""
        # Mock the GCP-specific parts
        mock_storage_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_init_gcs.return_value = mock_storage_client
        
        mock_iceberg_catalog = MagicMock()
        mock_table = MagicMock()
        mock_iceberg_catalog.load_table.return_value = mock_table
        mock_init_catalog.return_value = mock_iceberg_catalog
        
        # Run the function with real API call but mocked GCP services
        result = process_stations_data()
        
        # Assert that we got data back
        self.assertIsNotNone(result)
        self.assertGreater(len(result), 0)
        
        # Check that the expected fields are present
        expected_columns = ['station_id', 'latitude', 'longitude', 'elevation', 
                           'name', 'country', 'state', 'first_year', 'last_year']
        for col in expected_columns:
            self.assertIn(col, result.columns)
            
        # Check that the Iceberg append method was called
        mock_table.append.assert_called_once()

    @patch('src.batch_loader.init_gcs_client')
    @patch('src.batch_loader.init_iceberg_catalog')
    def test_process_observations_data_integration(self, mock_init_catalog, mock_init_gcs):
        """Test processing observations data with actual API calls for a limited dataset"""
        # Mock the GCP-specific parts
        mock_storage_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_init_gcs.return_value = mock_storage_client
        
        mock_iceberg_catalog = MagicMock()
        mock_table = MagicMock()
        mock_iceberg_catalog.load_table.return_value = mock_table
        mock_init_catalog.return_value = mock_iceberg_catalog
        
        # Run the function with real API call but mocked GCP services
        # Using a recent year but with a small MAX_RECORDS limit for the test
        with patch.dict(os.environ, {'MAX_RECORDS_PER_BATCH': '100'}):
            result = process_observations_data(2022)
        
        # Assert that we got data back
        self.assertIsNotNone(result)
        self.assertGreater(result, 0)
        self.assertLessEqual(result, 100)  # Should respect the limit
        
        # Check that the Iceberg append method was called
        mock_table.append.assert_called_once()

    def test_init_gcs_client_integration(self):
        """Test initializing GCS client with actual credentials"""
        # This will actually connect to GCS using the provided credentials
        client = init_gcs_client()
        self.assertIsNotNone(client)

    def test_init_iceberg_catalog_integration(self):
        """Test initializing Iceberg catalog with actual configuration"""
        # This will patch certain methods to avoid actual GCP calls
        with patch('pyiceberg.catalog.load_catalog') as mock_load_catalog:
            mock_catalog = MagicMock()
            mock_load_catalog.return_value = mock_catalog
            
            catalog = init_iceberg_catalog()
            self.assertEqual(catalog, mock_catalog)
            
            # Check that load_catalog was called with expected args
            mock_load_catalog.assert_called_once()
            args, kwargs = mock_load_catalog.call_args
            self.assertEqual(kwargs['type'], 'rest')
            self.assertTrue(kwargs['uri'].startswith('gs://'))
            self.assertTrue(kwargs['warehouse'].startswith('gs://'))
            self.assertEqual(kwargs['credential'], 'gcp')

    @patch('src.batch_loader.load_env_vars')
    @patch('src.batch_loader.process_stations_data')
    @patch('src.batch_loader.process_observations_data')
    @patch('src.batch_loader.parse_args')
    def test_main_integration(self, mock_parse_args, mock_process_observations, 
                            mock_process_stations, mock_load_env):
        """Test the main function with dependency injection"""
        # Mock the arguments
        mock_args = MagicMock()
        mock_args.year = 2022
        mock_args.stations_only = False
        mock_args.filter_stations = True
        mock_parse_args.return_value = mock_args
        
        # Mock the processing functions
        mock_stations_df = MagicMock()
        mock_process_stations.return_value = mock_stations_df
        mock_process_observations.return_value = 1000
        
        # Run the main function
        main()
        
        # Assert that the functions were called with expected args
        mock_load_env.assert_called_once()
        mock_process_stations.assert_called_once()
        mock_process_observations.assert_called_once_with(2022, mock_stations_df)
        

if __name__ == '__main__':
    unittest.main() 