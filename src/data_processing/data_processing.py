#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_module_description"""
# ---------------------------------------------------------------------------
import yaml
import re
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class Shot:

    def __init__(self, tags:dict, start_time:str, stop_time:str):
        
        date_fmt = '%Y-%m-%dT%H:%M:%SZ'
        self.tags = tags
        self.start_time = datetime.strptime(start_time, date_fmt)
        self.stop_time = datetime.strptime(stop_time, date_fmt)

    def __lt__(self, other):
        return self.start_time < other.start_time
    
    def __eq__(self, other):
        return self.start_time == other.start_time
    
    def __gt__(self, other):
        return self.start_time > other.start_time


class Shots:

    def __init__(self, filepath:str):
        
        with open(filepath, 'r') as file:
            self.shot_list = yaml.safe_load(file)['shots']

        # Sorting the list based on the start time
        self.shot_list = sorted(self.shot_list, 
                                key = lambda x: x['time']['start'])
        self.list_length = len(self.shot_list)


    def get_tags(self, index:int):
        try:
            return self.shot_list[index]['tags']
        except IndexError:
            logger.debug(
                f'Index of {index} is out of range (0 to {self.list_length - 1})'
                )


class Measurement:
    '''
    Object that holds the dict values relating to a single timestamp

    Attributes
    ----------
        timestamp_id : str
            timestamp id to scrape correct time value
        dict_template : dict
            dict where the keys are all the ids for values to be scraped 
            corresponding to the timestamp id and the values are strings
            that determine the type of variable the values will be
        id_list : list
            list of all the ids within the object in the order: 
                [timestamp_id, dict_template keys]
        time : datetime
            datetime of last stored data
        data : dict
            dict that stores the data from the last scrape. Matches the 
            structure of dict_template, with the values containing the actual 
            data in the type specified by the template
    ''' 

    def __init__(self, timestamp:str, dict_template:dict):
        self.timestamp_id = timestamp
        self.dict_template = dict_template
        self.id_list = [timestamp]

        for key, value in dict_template.items():
                self.id_list.append(key)

        self.time = None
        self.data = {}


class Measurements:
    '''
    Contains a list of Measurement objects for each timestamp id parsed from
    the measurements.yaml file

    Attributes
    ----------
        measurement_list : list
            list containing Measurement objects
        full_id_list : list
            list of all the ids from all Measurement objects
        length : int
            length of measurement_list for iterating through the Measurement 
            objects
    '''

    def __init__(self, filepath:str):
        
        self.measurement_list = []  # List of Measurement objects
        self.full_id_list = []  # List of ids from all Measurement objects

        with open(filepath, "r") as file:
            data_format = yaml.safe_load(file)

        # yaml file has the format:
        #     {timestamp_1: {data_1}, timestamp_2: {data_2}, ..., timestamp_n:{data_n}}
        # so the key will be all the timestamps and the values will be the 
        # dicts with all the data to be scraped for wach timestamp
        for key, value in data_format.items():
            measurement = Measurement(key, value)
            self.measurement_list.append(measurement)
            self.full_id_list += measurement.id_list

        # Length of self.measurement list for easier iteration through all 
        # Measurement objects
        self.length = len(self.measurement_list)  


    def convert_to_float(self, string):
        
        # Use regular expression to remove non-numeric characters
        numeric_string = re.sub(r"[^0-9.]+", "", string)
        try:
            result_float = float(numeric_string)
        except ValueError:
            logger.warning(f'{numeric_string} cannot be converted to float. Ignoring result')
            result_float = None
        return result_float


    def convert_to_date(self, value):
        
        date_fmt = "%Y-%m-%d %H:%M:%S"
        # Assuming value is a string representation of a datetime
        try:
            date = datetime.strptime(value, date_fmt)
        except ValueError:
            logger.warning(f'{value} cannot be converted to datetime. Ignoring result')
            date = None
        return date


    def convert_values(self, value, value_type):

        if value_type == 'float':
            return self.convert_to_float(value)
        elif value_type == 'string':
            return str(value)
        else:
            # Handle other categories as needed
            return value


    def store_data(self, raw_data:dict):
        '''
        Reads in data passed scraped from html based on the full_id_list and 
        properly stores it into each Measurement object
        '''
        # Iterating through each Measurement object in list
        for measurement in self.measurement_list:
            # Re-initializing the data to an empty dict
            measurement.data = {}
            
            # Iterating through each id as a key:value pair from scraped data
            for key, value in raw_data.items():
                # Checks if the current measurement object's timestamp matches
                # scraped data's id
                if measurement.timestamp_id == key:
                    # Converts the scraped time string into datetime object
                    time = self.convert_to_date(value)
                    # Check if updated time scraped is newer than previous
                    if (time == measurement.time) or (time is None):
                        # Do not update data. Results in a blank dict which
                        # does not update InfluxDB with the write_metric()
                        # function
                        break
                    else:
                        # New time found, updating time in Measurement object
                        measurement.time = time
                # Measurement's timestamp id not found, checking scraped
                # data's id to the keys in Measurement object
                # NOTE: Issues may arise if there are non-unique ids in html
                #       across multiple timestamps
                else:
                    # Pull all ids (k) and their data type (v) from
                    # the Measurement object 
                    for k, v in measurement.dict_template.items():
                        # Check if Measurement object's id matches scraped
                        # data's id
                        if k == key:
                            # Convert scraped data value to type based on
                            # template
                            corrected_value = self.convert_values(value, v)
                            # Update the Measurement object's data dict
                            measurement.data.update({k:corrected_value})


    def get_data(self, index:int):
        time = self.measurement_list[index].time
        data = self.measurement_list[index].data
        return data, time