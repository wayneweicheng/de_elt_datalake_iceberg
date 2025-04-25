"""
Integration tests for stream_processor.py

Note: These tests interact with actual GCP resources,
so they should be run in a test environment with proper credentials.
"""

import os
import sys
import json
import base64
import unittest
import tempfile
from datetime import datetime
from unittest.mock import patch, MagicMock

# Add the parent directory to sys.path to allow importing from src
import sys
from pathlib import Path

parent_dir = str(Path(__file__).resolve().parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from src.stream_processor import (
    load_env_vars,
    init_gcs_client,
    init_iceberg_catalog,
    validate_message,
    process_message,
    process_pubsub_message,
    create_test_message,
    simulate_pubsub_message,
    main
)


class TestStreamProcessorIntegration(unittest.TestCase):
    """Integration tests for stream_processor.py"""

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
        
        # Create a test message file
        cls.test_message_file = os.path.join(cls.test_dir.name, 'test_message.json')
        cls.test_message = {
            "station_id": "USW00094728",
            "date": datetime.now().strftime('%Y-%m-%d'),
            "element": "TMAX",
            "value": 32.5,
            "measurement_flag": "",
            "quality_flag": "",
            "source_flag": "S",
            "observation_time": datetime.now().strftime('%H:%M')
        }
        with open(cls.test_message_file, 'w') as f:
            json.dump(cls.test_message, f)

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

    def test_validate_message(self):
        """Test validating a message"""
        # Test a valid message
        valid_message = self.test_message.copy()
        result = validate_message(valid_message)
        self.assertTrue(result)
        
        # Test an invalid message (missing required field)
        invalid_message = self.test_message.copy()
        del invalid_message['station_id']
        result = validate_message(invalid_message)
        self.assertFalse(result)
        
        # Test an invalid message (wrong data type)
        invalid_message = self.test_message.copy()
        invalid_message['value'] = 'not a number'
        result = validate_message(invalid_message)
        self.assertFalse(result)

    @patch('src.stream_processor.init_gcs_client')
    @patch('src.stream_processor.init_iceberg_catalog')
    def test_process_message_integration(self, mock_init_catalog, mock_init_gcs):
        """Test processing a message with mocked GCP interactions"""
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
        
        # Process a valid message
        result = process_message(self.test_message)
        
        # Assert success
        self.assertTrue(result)
        
        # Check that the Iceberg append method was called
        mock_table.append.assert_called_once()
        
        # Check the file was uploaded to GCS
        mock_blob.upload_from_filename.assert_called_once()

    def test_process_pubsub_message_integration(self):
        """Test processing a Pub/Sub message"""
        # Create a mock Pub/Sub event
        pubsub_message_bytes = json.dumps(self.test_message).encode('utf-8')
        b64_message = base64.b64encode(pubsub_message_bytes).decode('utf-8')
        
        event = {
            'message': {
                'data': b64_message
            }
        }
        
        # Process the message with mocked GCP interactions
        with patch('src.stream_processor.process_message') as mock_process:
            mock_process.return_value = True
            response, status_code = process_pubsub_message(event)
        
        # Assert the response
        self.assertEqual(status_code, 200)
        self.assertEqual(response, "Success")

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

    def test_create_test_message(self):
        """Test creating a test message"""
        message = create_test_message()
        
        # Check that the message has all required fields
        required_fields = ['station_id', 'date', 'element', 'value']
        for field in required_fields:
            self.assertIn(field, message)
        
        # Check that the date is in the correct format
        try:
            datetime.strptime(message['date'], '%Y-%m-%d')
        except ValueError:
            self.fail("Date is not in the correct format")

    @patch('src.stream_processor.simulate_pubsub_message')
    @patch('src.stream_processor.load_env_vars')
    @patch('src.stream_processor.parse_args')
    def test_main_integration_with_test_flag(self, mock_parse_args, mock_load_env, 
                                           mock_simulate):
        """Test the main function with --test flag"""
        # Mock the arguments
        mock_args = MagicMock()
        mock_args.test = True
        mock_args.input_file = None
        mock_parse_args.return_value = mock_args
        
        # Run the main function
        main()
        
        # Assert function calls
        mock_load_env.assert_called_once()
        mock_simulate.assert_called_once()
        
        # Check that the first arg to simulate_pubsub_message is a dict with required fields
        args, kwargs = mock_simulate.call_args
        test_message = args[0]
        self.assertIsInstance(test_message, dict)
        required_fields = ['station_id', 'date', 'element', 'value']
        for field in required_fields:
            self.assertIn(field, test_message)

    @patch('src.stream_processor.simulate_pubsub_message')
    @patch('src.stream_processor.load_env_vars')
    @patch('src.stream_processor.parse_args')
    def test_main_integration_with_input_file(self, mock_parse_args, mock_load_env, 
                                            mock_simulate):
        """Test the main function with --input-file flag"""
        # Mock the arguments
        mock_args = MagicMock()
        mock_args.test = False
        mock_args.input_file = self.test_message_file
        mock_parse_args.return_value = mock_args
        
        # Run the main function
        main()
        
        # Assert function calls
        mock_load_env.assert_called_once()
        mock_simulate.assert_called_once()
        
        # Check that the first arg to simulate_pubsub_message is our test message
        args, kwargs = mock_simulate.call_args
        passed_message = args[0]
        self.assertEqual(passed_message, self.test_message)


if __name__ == '__main__':
    unittest.main() 