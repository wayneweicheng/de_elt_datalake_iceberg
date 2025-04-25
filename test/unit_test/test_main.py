"""
Unit tests for main.py Cloud Functions
"""

import os
import sys
import unittest
import json
import base64
from unittest.mock import patch, MagicMock, Mock

# Add the parent directory to sys.path to allow importing from src
import sys
from pathlib import Path

parent_dir = str(Path(__file__).resolve().parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# Import the functions to test, patching the dependencies first
with patch('src.main.functions_framework'):
    from src.main import climate_data_loader, process_climate_data_event


class TestCloudFunctions(unittest.TestCase):
    """Test cases for Cloud Functions in main.py"""

    @patch('src.main.load_env_vars')
    @patch('src.main.process_stations_data')
    @patch('src.main.process_observations_data')
    def test_climate_data_loader_with_year(self, mock_process_observations, mock_process_stations, mock_load_env):
        """Test climate_data_loader with year specified"""
        # Setup mocks
        mock_process_stations.return_value = MagicMock()
        mock_process_stations.return_value.__len__.return_value = 100
        mock_process_observations.return_value = 1000
        
        # Create mock request with year param
        mock_request = Mock()
        mock_request.get_json.return_value = {'year': 2022}
        mock_request.args = {}
        
        # Call the function
        response, status_code = climate_data_loader(mock_request)
        
        # Assert results
        mock_load_env.assert_called_once()
        mock_process_stations.assert_called_once()
        mock_process_observations.assert_called_once_with(2022)
        self.assertEqual(status_code, 200)
        self.assertIn('Processed 100 stations and 1000 climate data records for year 2022', response)

    @patch('src.main.load_env_vars')
    @patch('src.main.process_stations_data')
    @patch('src.main.process_observations_data')
    def test_climate_data_loader_stations_only(self, mock_process_observations, mock_process_stations, mock_load_env):
        """Test climate_data_loader with load_stations_only flag"""
        # Setup mocks
        mock_process_stations.return_value = MagicMock()
        mock_process_stations.return_value.__len__.return_value = 100
        
        # Create mock request with load_stations_only param
        mock_request = Mock()
        mock_request.get_json.return_value = {'load_stations_only': True}
        mock_request.args = {}
        
        # Call the function
        response, status_code = climate_data_loader(mock_request)
        
        # Assert results
        mock_load_env.assert_called_once()
        mock_process_stations.assert_called_once()
        mock_process_observations.assert_not_called()
        self.assertEqual(status_code, 200)
        self.assertIn('Processed 100 stations', response)

    @patch('src.main.load_env_vars')
    @patch('src.main.process_stations_data')
    @patch('src.main.process_observations_data')
    @patch('src.main.datetime')
    def test_climate_data_loader_default_year(self, mock_datetime, mock_process_observations, 
                                            mock_process_stations, mock_load_env):
        """Test climate_data_loader with default year (previous year)"""
        # Setup mocks
        mock_process_stations.return_value = MagicMock()
        mock_process_stations.return_value.__len__.return_value = 100
        mock_process_observations.return_value = 1000
        mock_datetime.datetime.now.return_value.year = 2023
        
        # Create mock request without year param
        mock_request = Mock()
        mock_request.get_json.return_value = {}
        mock_request.args = {}
        
        # Call the function
        response, status_code = climate_data_loader(mock_request)
        
        # Assert results
        mock_load_env.assert_called_once()
        mock_process_stations.assert_called_once()
        mock_process_observations.assert_called_once_with(2022)  # 2023 - 1 = 2022
        self.assertEqual(status_code, 200)
        self.assertIn('Processed 100 stations and 1000 climate data records for year 2022', response)

    @patch('src.main.load_env_vars')
    @patch('src.main.process_stations_data')
    def test_climate_data_loader_error(self, mock_process_stations, mock_load_env):
        """Test climate_data_loader with error handling"""
        # Setup mocks to raise an exception
        mock_process_stations.side_effect = Exception("Test error")
        
        # Create mock request
        mock_request = Mock()
        mock_request.get_json.return_value = {}
        mock_request.args = {}
        
        # Call the function
        response, status_code = climate_data_loader(mock_request)
        
        # Assert results
        mock_load_env.assert_called_once()
        mock_process_stations.assert_called_once()
        self.assertEqual(status_code, 500)
        self.assertIn('Error: Test error', response)

    @patch('src.main.process_pubsub_message')
    def test_process_climate_data_event(self, mock_process_pubsub):
        """Test process_climate_data_event"""
        # Setup mocks
        mock_process_pubsub.return_value = ('Success', 200)
        
        # Create a mock CloudEvent
        encoded_data = base64.b64encode(b'{"test": "data"}').decode('utf-8')
        mock_cloud_event = Mock()
        mock_cloud_event.data = {
            'message': {
                'data': encoded_data
            }
        }
        
        # Call the function
        response, status_code = process_climate_data_event(mock_cloud_event)
        
        # Assert results
        mock_process_pubsub.assert_called_once_with({
            'message': {
                'data': encoded_data
            }
        })
        self.assertEqual(response, 'Success')
        self.assertEqual(status_code, 200)

    @patch('src.main.process_pubsub_message')
    def test_process_climate_data_event_error(self, mock_process_pubsub):
        """Test process_climate_data_event with error handling"""
        # Setup mocks to raise an exception
        mock_process_pubsub.side_effect = Exception("Test error")
        
        # Create a mock CloudEvent
        encoded_data = base64.b64encode(b'{"test": "data"}').decode('utf-8')
        mock_cloud_event = Mock()
        mock_cloud_event.data = {
            'message': {
                'data': encoded_data
            }
        }
        
        # Call the function
        response, status_code = process_climate_data_event(mock_cloud_event)
        
        # Assert results
        mock_process_pubsub.assert_called_once()
        self.assertEqual(status_code, 500)
        self.assertIn('Error: Test error', response)


if __name__ == '__main__':
    unittest.main() 