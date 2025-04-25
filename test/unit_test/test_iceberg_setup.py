"""
Unit tests for iceberg_setup.py
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock, call

# Add the parent directory to sys.path to allow importing from src
import sys
from pathlib import Path

parent_dir = str(Path(__file__).resolve().parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from src.iceberg_setup import (
    load_env_vars,
    create_catalog_config,
    setup_iceberg_catalog,
    create_namespace,
    define_stations_schema,
    define_observations_schema,
    create_table,
    main
)


class TestIcebergSetup(unittest.TestCase):
    """Test cases for iceberg_setup.py module"""

    @patch('src.iceberg_setup.load_dotenv')
    @patch('src.iceberg_setup.Path.exists')
    def test_load_env_vars(self, mock_exists, mock_load_dotenv):
        """Test loading environment variables"""
        # Mock the .env file existence
        mock_exists.return_value = True
        
        # Call the function
        load_env_vars()
        
        # Assert load_dotenv was called
        mock_load_dotenv.assert_called_once()
        
        # Test when .env doesn't exist
        mock_exists.return_value = False
        mock_load_dotenv.reset_mock()
        
        load_env_vars()
        # Should still call load_dotenv even if file doesn't exist
        mock_load_dotenv.assert_called_once()

    @patch.dict(os.environ, {
        'ICEBERG_CATALOG_BUCKET': 'test-catalog-bucket',
        'ICEBERG_TABLES_BUCKET': 'test-tables-bucket',
        'GCP_REGION': 'test-region'
    })
    def test_create_catalog_config(self):
        """Test creating catalog configuration"""
        config = create_catalog_config()
        
        # Check config values
        self.assertEqual(config['type'], 'rest')
        self.assertEqual(config['uri'], 'gs://test-catalog-bucket')
        self.assertEqual(config['warehouse'], 'gs://test-tables-bucket')
        self.assertEqual(config['credential'], 'gcp')
        self.assertEqual(config['region'], 'test-region')

    @patch('src.iceberg_setup.load_catalog')
    @patch('src.iceberg_setup.create_catalog_config')
    def test_setup_iceberg_catalog(self, mock_create_config, mock_load_catalog):
        """Test setting up Iceberg catalog"""
        # Setup mocks
        mock_create_config.return_value = {
            'type': 'rest',
            'uri': 'gs://test-catalog-bucket',
            'warehouse': 'gs://test-tables-bucket',
            'credential': 'gcp',
            'region': 'test-region'
        }
        mock_catalog = MagicMock()
        mock_load_catalog.return_value = mock_catalog
        
        # Call the function
        result = setup_iceberg_catalog('test_catalog')
        
        # Assert results
        mock_create_config.assert_called_once()
        mock_load_catalog.assert_called_once_with('test_catalog', **mock_create_config.return_value)
        self.assertEqual(result, mock_catalog)

    def test_define_stations_schema(self):
        """Test defining the stations schema"""
        schema = define_stations_schema()
        
        # Check schema structure
        self.assertEqual(schema['type'], 'struct')
        self.assertEqual(len(schema['fields']), 9)
        
        # Check required fields
        station_id_field = next(f for f in schema['fields'] if f['name'] == 'station_id')
        self.assertTrue(station_id_field['required'])

    def test_define_observations_schema(self):
        """Test defining the observations schema"""
        schema = define_observations_schema()
        
        # Check schema structure
        self.assertEqual(schema['type'], 'struct')
        self.assertEqual(len(schema['fields']), 8)
        
        # Check required fields
        required_field_names = ['station_id', 'date', 'element']
        for name in required_field_names:
            field = next(f for f in schema['fields'] if f['name'] == name)
            self.assertTrue(field['required'])

    @patch('src.iceberg_setup.setup_iceberg_catalog')
    @patch('src.iceberg_setup.create_namespace')
    @patch('src.iceberg_setup.create_table')
    @patch('src.iceberg_setup.define_stations_schema')
    @patch('src.iceberg_setup.define_observations_schema')
    @patch('src.iceberg_setup.load_env_vars')
    def test_main(self, mock_load_env, mock_observations_schema, mock_stations_schema, 
                 mock_create_table, mock_create_namespace, mock_setup_catalog):
        """Test the main function"""
        # Setup mocks
        mock_catalog = MagicMock()
        mock_setup_catalog.return_value = mock_catalog
        mock_stations_schema.return_value = {"type": "struct", "fields": []}
        mock_observations_schema.return_value = {"type": "struct", "fields": []}
        
        # Call the main function
        with patch('sys.argv', ['iceberg_setup.py']):
            main()
        
        # Assert function calls
        mock_load_env.assert_called_once()
        mock_setup_catalog.assert_called_once()
        mock_create_namespace.assert_called_once()
        
        # Check that create_table was called twice (stations and observations)
        self.assertEqual(mock_create_table.call_count, 2)


if __name__ == '__main__':
    unittest.main() 