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
            list = yaml.safe_load(file)['shots']

        self.shot_list = []
        for shot in list:
            tags = shot['tags']
            start_time = shot['time']['start']
            stop_time = shot['time']['stop']
            s = Shot(tags, start_time, stop_time)
            self.shot_list.append(s)
        self.shot_list = sorted(self.shot_list)
        
        self.length = len(self.shot_list)


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
        result_float = float(numeric_string)

        return result_float


    def convert_to_date(self, value):
        
        date_fmt = "%Y-%m-%d %H:%M:%S"
        # Assuming value is a string representation of a datetime
        date = datetime.strptime(value, date_fmt)

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
                        logger.info(f'Time of {time} was eiter not found or not updated from previous request.')
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
                            try:
                                # Convert scraped data value to type based on
                                # template
                                corrected_value = self.convert_values(value, v)
                            except ValueError:
                                logger.warning(f'{value} cannot be converted to {v}. Ignoring result')
                                break
                            # Update the Measurement object's data dict
                            measurement.data.update({k:corrected_value})


    def get_data(self, index:int):
        time = self.measurement_list[index].time
        data = self.measurement_list[index].data
        return data, time
    

class DataPoint:

    def __init__(self, name:str, value:dict):
        self.name = name
        self.data_type = value['data_type']
        self.value = None
        self.time = None

        try:
            self.source_id = value['source_id']
        except KeyError:
            self.source_id = name

        try:
            self.tags = value['tags']
        except KeyError:
            self.tags = None

        try:
            self.time_id = value['source_time']
        except KeyError:
            self.time_id = None
        

class DataList:

    def __init__(self, measurement_filepath:str, shot_filepath:str):

        self.data_list = []  # List of Measurement objects
        self.id_list = []  # List of ids from all DataPoints
        self.time_id_list = [] # List of timestamp ids
        self.timestamps = {}

        # Initializing information for holding data
        with open(measurement_filepath, "r") as file:
            data_format = yaml.safe_load(file)

        for key, value in data_format.items():
            if key == 'timestamps':
                for element in value:
                    self.timestamps.update({element: None})
                    self.time_id_list.append(element)
            else:  
                point = DataPoint(key, value)
                self.data_list.append(point)
                self.id_list.append(point.source_id)

        self.length = len(self.data_list)

        # Grabbing time and additional tags used from the shot
        with open(shot_filepath, 'r') as file:
            shot = yaml.safe_load(file)
        try:
            self.shot_tags = shot['tags']
        except KeyError:
            self.shot_tags = None
        date_fmt = '%Y-%m-%dT%H:%M:%SZ'
        self.shot_start_time_str = shot['time']['start']
        self.shot_stop_time_str = shot['time']['stop']
        self.shot_start_time = datetime.strptime(self.shot_start_time_str, date_fmt)
        self.shot_stop_time = datetime.strptime(self.shot_stop_time_str, date_fmt)

    
    def convert_to_float(self, string):
        
        # Use regular expression to remove non-numeric characters
        numeric_string = re.sub(r"[^0-9.]+", "", string)
        result_float = float(numeric_string)

        return result_float


    def convert_to_date(self, value):
        
        date_fmt = "%Y-%m-%d %H:%M:%S"
        # Assuming value is a string representation of a datetime
        date = datetime.strptime(value, date_fmt)

        return date


    def convert_values(self, value, value_type):

        if value_type == 'float':
            return self.convert_to_float(value)
        elif value_type == 'string':
            return str(value)
        else:
            # Handle other categories as needed
            return value


    def store_data(self, scraped_data:dict, scraped_time:dict):
        '''
        Reads in data passed scraped from html based on the full_id_list and 
        properly stores it into each Measurement object
        '''
        # Initialize update flags to false
        update_flag = {}
        for time_id, timestamp in self.timestamps.items():
            update_flag.update({time_id:False})

        # Update timestamps if new and set update flag to true
        for time_id, timestamp in scraped_time.items():
            if (timestamp != self.timestamps[time_id]) and (timestamp is not None):
                update_flag.update({time_id: True})
                self.timestamps.update({time_id:timestamp})       
        
        # Loop for updating all the DataPoints in the list
        for datapoint in self.data_list:
            datapoint.value = None 
            # Update values using current time if timestamp not specified
            if datapoint.time_id is None:
                datapoint.time = datetime.now()
                for value_id, value_data in scraped_data.items():
                    if value_id == datapoint.source_id:
                        try:
                            datapoint.value = self.convert_values(value_data, datapoint.data_type)
                        except ValueError:
                            logger.warning(f'{value_data} cannot be converted to {datapoint.data_type}. Ignoring result')
                        break

            # Update values if update flag is set to True for DataPoint's given timestamp
            elif (update_flag[datapoint.time_id]):
                for value_id, value_data in scraped_data.items():
                    if value_id == datapoint.source_id:
                        try:
                            datapoint.value = self.convert_values(value_data, datapoint.data_type)
                        except ValueError:
                            logger.warning(f'{value_data} cannot be converted to {datapoint.data_type}. Ignoring result')
                        break