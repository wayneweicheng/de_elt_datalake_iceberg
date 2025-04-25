#!/usr/bin/env python
"""
main.py - Cloud Function entry points for climate data processing

This module provides entry points for Google Cloud Functions that handle
batch extraction and streaming processing of climate data.
"""

import os
import sys
import json
import logging
import datetime
from pathlib import Path
from typing import Dict, Any, Tuple, Union

# Add the parent directory to sys.path to allow importing from src
parent_dir = str(Path(__file__).resolve().parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import functions_framework
from dotenv import load_dotenv

# Import local modules
from src.batch_loader import process_stations_data, process_observations_data, load_env_vars
from src.stream_processor import process_pubsub_message

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


@functions_framework.http
def climate_data_loader(request) -> Tuple[str, int]:
    """
    HTTP Cloud Function that triggers the climate data loading process.
    
    Args:
        request: HTTP request object
    Returns:
        Response text and status code
    """
    try:
        # Load environment variables
        load_env_vars()
        
        # Parse request parameters
        request_json = request.get_json(silent=True)
        request_args = request.args
        
        # Check for load_stations_only flag
        load_stations_only = False
        if request_json and 'load_stations_only' in request_json:
            load_stations_only = bool(request_json['load_stations_only'])
        elif request_args and 'load_stations_only' in request_args:
            load_stations_only = request_args['load_stations_only'].lower() == 'true'
        
        # Process stations data
        logger.info("Processing stations data")
        stations_df = process_stations_data()
        
        # If stations-only flag is set, return early
        if load_stations_only:
            return f"Processed {len(stations_df)} stations", 200
        
        # Check if specific year, month, day were provided
        year = None
        if request_json and 'year' in request_json:
            year = int(request_json['year'])
        elif request_args and 'year' in request_args:
            year = int(request_args['year'])
        
        # If no year specified, use previous year
        if year is None:
            year = datetime.datetime.now().year - 1
            logger.info(f"No year specified, defaulting to previous year: {year}")
        
        # Check if filtering by stations is requested
        filter_stations = False
        if request_json and 'filter_stations' in request_json:
            filter_stations = bool(request_json['filter_stations'])
        elif request_args and 'filter_stations' in request_args:
            filter_stations = request_args['filter_stations'].lower() == 'true'
        
        # Process observations
        logger.info(f"Processing observations for year {year}")
        if filter_stations:
            logger.info("Filtering observations by stations")
            processed_records = process_observations_data(year, stations_df)
        else:
            processed_records = process_observations_data(year)
        
        return f"Processed {len(stations_df)} stations and {processed_records} climate data records for year {year}.", 200
    
    except Exception as e:
        logger.error(f"Error in climate_data_loader: {e}")
        return f"Error: {str(e)}", 500


@functions_framework.cloud_event
def process_climate_data_event(cloud_event) -> Tuple[str, int]:
    """
    Cloud Function triggered by Pub/Sub events for climate data streaming.
    
    Args:
        cloud_event: Cloud event object containing Pub/Sub message
    Returns:
        Response text and status code
    """
    try:
        # Extract the Pub/Sub message from the CloudEvent
        event = {
            'message': {
                'data': cloud_event.data["message"]["data"]
            }
        }
        
        # Process the Pub/Sub message
        return process_pubsub_message(event)
    
    except Exception as e:
        logger.error(f"Error in process_climate_data_event: {e}")
        return f"Error: {str(e)}", 500


# For local testing
if __name__ == "__main__":
    # Test the climate_data_loader function with a mock request
    class MockRequest:
        def __init__(self, json_data=None, args=None):
            self.json_data = json_data
            self.args = args
        
        def get_json(self, silent=False):
            return self.json_data
    
    # Example: Test with a specific year
    mock_request = MockRequest(json_data={"year": 2022})
    response, status_code = climate_data_loader(mock_request)
    print(f"Response: {response}, Status: {status_code}") 