## @package gtfs_lib
# Application library containing the Classes and functions needed to aggregate and manage GTFS-RT data from OBCs
import copy
import datetime
import gc
import ipaddress
import os.path
import time
import datetime
from copy import deepcopy
from queue import Queue
from typing import Union, Any, Dict
from threading import Lock, RLock
import threading
import requests
import json
from json import JSONDecodeError
import pathlib
import traceback
import queue
import mariadb
from protobuf.protoc_python import gtfs_spec_pb2
from enum import IntEnum
from geopy.distance import geodesic
import re
import random
import warnings
import logging
import logging.handlers
import multiprocessing as mp
from functools import lru_cache,cache,cached_property

logger = logging.getLogger()


def init_main(log_size: int, backups: int, debug: bool,log_queue:mp.Queue) -> Union[tuple[logging.Logger,logging.handlers.QueueListener], None]:
    """

    Initialize the main logger.

    Parameters:
        log_size (int): The maximum size of each log file in bytes.
        backups (int): The number of backup log files to keep.
        debug (bool): Flag indicating whether debug mode is enabled.

    Returns:
        Union[logging.Logger, None]: The initialized logger object if the method is executed in the main thread,
            otherwise None.

    """
    global logger
    if threading.current_thread() is threading.main_thread():
        if debug is None:
            debug = False
        lvl = logging.INFO if not debug else logging.DEBUG
        logging.basicConfig(level=lvl, filemode="w", encoding='utf-8')
        ch = logging.StreamHandler()
        ch.setLevel(lvl)
        formatter = logging.Formatter('[%(asctime)s] {PID:p%(process)s} {Process:%(processName)s} {Thread_id:%('
                                      'thread)d}  {%(threadName)s:%('
                                      'funcName)s}  {%(pathname)s:%(lineno)d}  %(levelname)s - %(message)s',
                                      '%d-%m-%Y %H:%M:%S')
        ch.setFormatter(formatter)

        fh = logging.handlers.RotatingFileHandler('server_log_debug.log', maxBytes=log_size, backupCount=backups,
                                                  encoding='utf-8')
        fh.setFormatter(formatter)
        fh.setLevel(lvl)
        logger.addHandler(ch)
        logger.addHandler(fh)
        warnings_logger = logging.getLogger("py.warnings")
        logging.captureWarnings(False)
        warnings_logger.addHandler(fh)
        warnings_logger.addHandler(ch)

        handler_log_th = logging.handlers.QueueListener(log_queue, fh,ch,respect_handler_level=True)
        handler_log_th.start()
        logger.info(f"Started")


        return logger,handler_log_th
    return None


class RuntimeError_MariaDB(RuntimeError):
    pass


## export_mem_object_th
#
# Function used to compile/encapsulate an object from the raw data returned from the th API
# This function populates with None attributes not found

def export_mem_object_th(obj_in, datetime_arg: datetime.datetime, vehicle_key: Union[None, int]) -> Union[
    dict[datetime.datetime, None, dict], dict[Union[str, int], Union[datetime.datetime, dict[Any, Any]]]]:
    """Export th memory object.

    Args:
        obj_in (dict): Input object containing th data.
        datetime_arg (datetime.datetime): Timestamp to be included in the return object.
        vehicle_key (None|int): Key for a specific vehicle. Set to None to export all vehicles.

    Returns:
        dict: Exported memory object.

    """
    return_object = dict()
    return_object['timestamp'] = datetime_arg
    # logger = logging.getLogger(__name__)
    if vehicle_key is None:
        for key in obj_in:
            if not str(key).isnumeric():
                continue
            key_nr = int(key)
            return_object[key_nr] = dict()
            obj_tmp = obj_in[key]

            if '0' in obj_tmp:
                try:
                    return_object[key_nr]['timestamp_reception'] = datetime.datetime.fromisoformat(obj_tmp['0'])
                except ValueError as err:
                    return_object[key_nr]['timestamp_reception'] = None


            else:
                return_object[key_nr]['timestamp_reception'] = None
            if '1' in obj_tmp:
                return_object[key_nr]['status'] = (obj_tmp['1'] == 'on')
            else:
                return_object[key_nr]['status'] = None
            if '2' in obj_tmp:
                try:
                    return_object[key_nr]['lat'] = float(str(obj_tmp['2']))
                except ValueError as err:
                    return_object[key_nr]['lat'] = None
                    # logger.info("Value error at %s in thread %(thread)d ", exc_info=err)
            else:
                return_object[key_nr]['lat'] = None
            if '3' in obj_tmp:
                try:
                    return_object[key_nr]['lon'] = float(str(obj_tmp['3']))
                except ValueError as err:
                    return_object[key_nr]['lon'] = None
                    # logger.info("Value error at %s in thread %(thread)d ", exc_info=err)
            else:
                return_object[key_nr]['lon'] = None
            if '4' in obj_tmp:
                try:
                    return_object[key_nr]['line_id'] = str(obj_tmp['4'])
                except ValueError as err:
                    return_object[key_nr]['line_id'] = None
            else:
                return_object[key_nr]['line_id'] = None
            if '5' in obj_tmp:
                try:
                    return_object[key_nr]['odometer'] = int(obj_tmp['5'])
                except ValueError as err:
                    return_object[key_nr]['odometer'] = None
            else:
                return_object[key_nr]['odometer'] = None
            if '6' in obj_tmp:
                try:
                    return_object[key_nr]['engine_rpm'] = float(obj_tmp['6'])
                except ValueError as err:
                    return_object[key_nr]['engine_rpm'] = None
            else:
                return_object[key_nr]['engine_rpm'] = None
            if '7' in obj_tmp:
                try:
                    return_object[key_nr]['engine_coolant_temp'] = float(obj_tmp['7'])
                except ValueError as err:
                    return_object[key_nr]['engine_coolant_temp'] = None
            else:
                return_object[key_nr]['engine_coolant_temp'] = None
            if '17' in obj_tmp:
                try:
                    return_object[key_nr]['tank_level'] = int(obj_tmp['17'])
                except ValueError as err:
                    return_object[key_nr]['tank_level'] = None
            else:
                return_object[key_nr]['tank_level'] = None
            if '9' in obj_tmp:
                try:
                    return_object[key_nr]['psu_level'] = int(obj_tmp['9'])
                except ValueError as err:
                    return_object[key_nr]['psu_level'] = None
            else:
                return_object[key_nr]['psu_level'] = None
            if '18' in obj_tmp:
                try:
                    return_object[key_nr]['aux_fuel_level'] = int(obj_tmp['18'])
                except ValueError as err:
                    return_object[key_nr]['aux_fuel_level'] = None
            else:
                return_object[key_nr]['aux_fuel_level'] = None
            if '50' in obj_tmp:
                try:
                    return_object[key_nr]['gps_qos'] = float(obj_tmp['50'])
                except ValueError as err:
                    return_object[key_nr]['gps_qos'] = None
            else:
                return_object[key_nr]['gps_qos'] = None
            if '49' in obj_tmp:
                try:
                    return_object[key_nr]['gps_sat_nr'] = int(obj_tmp['49'])
                except ValueError as err:
                    return_object[key_nr]['gps_sat_nr'] = None
            else:
                return_object[key_nr]['gps_sat_nr'] = None
            if '217' in obj_tmp:
                try:
                    return_object[key_nr]['ip_addr'] = ipaddress.ip_address(obj_tmp['217'])
                except ValueError as err:
                    return_object[key_nr]['ip_addr'] = None
            else:
                return_object[key_nr]['ip_addr'] = None
            if '19' in obj_tmp:
                try:
                    return_object[key_nr]['fuel_used'] = float(obj_tmp['19'])
                except ValueError as err:
                    return_object[key_nr]['fuel_used'] = None
            else:
                return_object[key_nr]['fuel_used'] = None
            if '10' in obj_tmp:
                try:
                    return_object[key_nr]['speed'] = float(obj_tmp['10'])
                except ValueError as err:
                    return_object[key_nr]['speed'] = None
            else:
                return_object[key_nr]['speed'] = None
            if '204' in obj_tmp:
                try:
                    return_object[key_nr]['speed_aux'] = float(obj_tmp['204'])
                except ValueError as err:
                    return_object[key_nr]['speed_aux'] = None
            else:
                return_object[key_nr]['speed_aux'] = None
            psg_count = 0
            psg_count_en = 0
            if '23' in obj_tmp:
                try:
                    return_object[key_nr]['psg_counter_1'] = int(obj_tmp['23'])
                    psg_count += int(obj_tmp['23'])
                    psg_count_en += 1
                except ValueError as err:
                    return_object[key_nr]['psg_counter_1'] = None
            else:
                return_object[key_nr]['psg_counter_1'] = None
            if '24' in obj_tmp:
                try:
                    return_object[key_nr]['psg_counter_2'] = int(obj_tmp['24'])
                    psg_count += int(obj_tmp['24'])
                    psg_count_en += 1
                except ValueError as err:
                    return_object[key_nr]['psg_counter_2'] = None
            else:
                return_object[key_nr]['psg_counter_2'] = None
            if '25' in obj_tmp:
                try:
                    return_object[key_nr]['psg_counter_3'] = int(obj_tmp['25'])
                    psg_count += int(obj_tmp['25'])
                    psg_count_en += 1
                except ValueError as err:
                    return_object[key_nr]['psg_counter_3'] = None
            else:
                return_object[key_nr]['psg_counter_3'] = None
            if '26' in obj_tmp:
                try:
                    return_object[key_nr]['psg_counter_4'] = int(obj_tmp['26'])
                    psg_count += int(obj_tmp['26'])
                    psg_count_en += 1
                except ValueError as err:
                    return_object[key_nr]['psg_counter_4'] = None
            else:
                return_object[key_nr]['psg_counter_4'] = None
            if '103' in obj_tmp:
                try:
                    return_object[key_nr]['psg_counter_5'] = int(obj_tmp['103'])
                    psg_count += int(obj_tmp['103'])
                    psg_count_en += 1
                except ValueError as err:
                    return_object[key_nr]['psg_counter_5'] = None
            else:
                return_object[key_nr]['psg_counter_5'] = None

            if psg_count_en > 0:
                return_object[key_nr]['psg_count_total'] = psg_count
            else:
                return_object[key_nr]['psg_count_total'] = None
    else:
        try:
            key = int(vehicle_key)
        except ValueError as err:
            print(f"Value Error in th data process:{err}")
            return export_mem_object_th(obj_in, datetime_arg, None)
        if key in obj_in:
            obj_tmp = obj_in[key]
            key_nr = int(key)
            return_object[key_nr] = dict()

            if '0' in obj_tmp:
                try:
                    return_object[key_nr]['timestamp_reception'] = datetime.datetime.fromisoformat(obj_tmp['0'])
                except ValueError as err:
                    return_object[key_nr]['timestamp_reception'] = None
            else:
                return_object[key_nr]['timestamp_reception'] = None
            if '1' in obj_tmp:
                return_object[key_nr]['status'] = (obj_tmp['1'] == 'on')
            else:
                return_object[key_nr]['status'] = None
            if '2' in obj_tmp:
                try:
                    return_object[key_nr]['lat'] = float(str(obj_tmp['2']))
                except ValueError as err:
                    return_object[key_nr]['lat'] = None
            else:
                return_object[key_nr]['lat'] = None
            if '3' in obj_tmp:
                try:
                    return_object[key_nr]['lon'] = float(str(obj_tmp['3']))
                except ValueError as err:
                    return_object[key_nr]['lon'] = None
            else:
                return_object[key_nr]['lon'] = None
            if '4' in obj_tmp:
                try:
                    return_object[key_nr]['line_id'] = str(obj_tmp['4'])
                except ValueError as err:
                    return_object[key_nr]['line_id'] = None
            else:
                return_object[key_nr]['line_id'] = None
            if '5' in obj_tmp:
                try:
                    return_object[key_nr]['odometer'] = int(obj_tmp['5'])
                except ValueError as err:
                    return_object[key_nr]['odometer'] = None
            else:
                return_object[key_nr]['odometer'] = None
            if '6' in obj_tmp:
                try:
                    return_object[key_nr]['engine_rpm'] = float(obj_tmp['6'])
                except ValueError as err:
                    return_object[key_nr]['engine_rpm'] = None
            else:
                return_object[key_nr]['engine_rpm'] = None
            if '7' in obj_tmp:
                try:
                    return_object[key_nr]['engine_coolant_temp'] = float(obj_tmp['7'])
                except ValueError as err:
                    return_object[key_nr]['engine_coolant_temp'] = None
            else:
                return_object[key_nr]['engine_coolant_temp'] = None
            if '17' in obj_tmp:
                try:
                    return_object[key_nr]['tank_level'] = int(obj_tmp['17'])
                except ValueError as err:
                    return_object[key_nr]['tank_level'] = None
            else:
                return_object[key_nr]['tank_level'] = None
            if '9' in obj_tmp:
                try:
                    return_object[key_nr]['psu_level'] = int(obj_tmp['9'])
                except ValueError as err:
                    return_object[key_nr]['psu_level'] = None
            else:
                return_object[key_nr]['psu_level'] = None
            if '18' in obj_tmp:
                try:
                    return_object[key_nr]['aux_fuel_level'] = int(obj_tmp['18'])
                except ValueError as err:
                    return_object[key_nr]['aux_fuel_level'] = None
            else:
                return_object[key_nr]['aux_fuel_level'] = None
            if '50' in obj_tmp:
                try:
                    return_object[key_nr]['gps_qos'] = float(obj_tmp['50'])
                except ValueError as err:
                    return_object[key_nr]['gps_qos'] = None
            else:
                return_object[key_nr]['gps_qos'] = None
            if '49' in obj_tmp:
                try:
                    return_object[key_nr]['gps_sat_nr'] = int(obj_tmp['49'])
                except ValueError as err:
                    return_object[key_nr]['gps_sat_nr'] = None
            else:
                return_object[key_nr]['gps_sat_nr'] = None
            if '217' in obj_tmp:
                try:
                    return_object[key_nr]['ip_addr'] = ipaddress.ip_address(obj_tmp['217'])
                except ValueError as err:
                    return_object[key_nr]['ip_addr'] = None
            else:
                return_object[key_nr]['ip_addr'] = None
            if '19' in obj_tmp:
                try:
                    return_object[key_nr]['fuel_used'] = float(obj_tmp['19'])
                except ValueError as err:
                    return_object[key_nr]['fuel_used'] = None
            else:
                return_object[key_nr]['fuel_used'] = None
            if '10' in obj_tmp:
                try:
                    return_object[key_nr]['speed'] = float(obj_tmp['10'])
                except ValueError as err:
                    return_object[key_nr]['speed'] = None
            else:
                return_object[key_nr]['speed'] = None
            if '204' in obj_tmp:
                try:
                    return_object[key_nr]['speed_aux'] = float(obj_tmp['204'])
                except ValueError as err:
                    return_object[key_nr]['speed_aux'] = None
            else:
                return_object[key_nr]['speed_aux'] = None
            psg_count = 0
            psg_count_en = 0
            if '23' in obj_tmp:
                try:
                    return_object[key_nr]['psg_counter_1'] = int(obj_tmp['23'])
                    psg_count += int(obj_tmp['23'])
                    psg_count_en += 1
                except ValueError as err:
                    return_object[key_nr]['psg_counter_1'] = None
            else:
                return_object[key_nr]['psg_counter_1'] = None
            if '24' in obj_tmp:
                try:
                    return_object[key_nr]['psg_counter_2'] = int(obj_tmp['24'])
                    psg_count += int(obj_tmp['24'])
                    psg_count_en += 1
                except ValueError as err:
                    return_object[key_nr]['psg_counter_2'] = None
            else:
                return_object[key_nr]['psg_counter_2'] = None
            if '25' in obj_tmp:
                try:
                    return_object[key_nr]['psg_counter_3'] = int(obj_tmp['25'])
                    psg_count += int(obj_tmp['25'])
                    psg_count_en += 1
                except ValueError as err:
                    return_object[key_nr]['psg_counter_3'] = None
            else:
                return_object[key_nr]['psg_counter_3'] = None
            if '26' in obj_tmp:
                try:
                    return_object[key_nr]['psg_counter_4'] = int(obj_tmp['26'])
                    psg_count += int(obj_tmp['26'])
                    psg_count_en += 1
                except ValueError as err:
                    return_object[key_nr]['psg_counter_4'] = None
            else:
                return_object[key_nr]['psg_counter_4'] = None
            if '103' in obj_tmp:
                try:
                    return_object[key_nr]['psg_counter_5'] = int(obj_tmp['103'])
                    psg_count += int(obj_tmp['103'])
                    psg_count_en += 1
                except ValueError as err:
                    return_object[key_nr]['psg_counter_5'] = None
            else:
                return_object[key_nr]['psg_counter_5'] = None

            if psg_count_en > 0:
                return_object[key_nr]['psg_count_total'] = psg_count
            else:
                return_object[key_nr]['psg_count_total'] = None
    return return_object


class GTFS_S_RequestClass:
    """
    Class GTFS_S_RequestClass

    This class is used to make requests to the GTFS transit feed API.

    Attributes:
        url (str): The URL of the API endpoint.
        headers (dict): The headers to be included in the request.
        cookies (dict): The cookies to be included in the request.
        session (object): The session to be used for making requests.
        kontor (int): The counter variable.

    Methods:
        request() -> dict:
            Sends a GET request to the API endpoint and returns a dictionary containing the response data.

        get_gtfs_feed_data() -> dict:
            Calls the request() method and returns the response data if the request is successful.

    Raises:
        InternalConnectionError: Raised if there is an error during the request.

    Usage:
        gtfs_request = GTFS_S_RequestClass()
        gtfs_data = gtfs_request.get_gtfs_feed_data()
    """
    def __init__(self):
        self.url = "<>"
        self.headers = {'Accept-Encoding': 'gzip,deflate,br',

                        'Cache-Control': 'no-cache',
                        'Connection': 'close',

                        'Accept': '*/*'}
        self.cookies = dict(ROUTEID='.2')
        self.session = requests.Session()
        self.kontor = 0

    def request(self) -> dict:
        """
        Sends a GET request to a specified URL using the provided headers, cookies, and session.

        Returns a dictionary containing the following information:
        - timestamp_utc: Current timestamp in UTC timezone
        - status: Boolean value indicating if the request was successful (True) or not (False)
        - data: Response content if the request was successful, otherwise None
        - status_code: HTTP status code returned from the request

        Exceptions are caught and converted into InternalConnectionErrors, which are then raised.

        """
        now = datetime.datetime.now(datetime.timezone.utc)
        try:
            x = None
            with self.session as session:
                x = session.request(method='GET', url=self.url, headers=self.headers, cookies=self.cookies, stream=True,
                                    timeout=10)
                self.kontor = self.kontor + 1
                stat = bool(x.status_code == requests.codes.ok)
                if stat:
                    ret_val = x.content
                else:
                    ret_val = None
                ret_obj = dict(timestamp_utc=now, status=stat, data=ret_val, status_code=x.status_code)
                x.close()

                return ret_obj
        # Converge all exceptions as InternalConnection Errors to outside
        except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout, requests.exceptions.InvalidURL,
                requests.exceptions.RequestException,
                requests.exceptions.BaseHTTPError, requests.exceptions.HTTPError, requests.exceptions.ReadTimeout,
                requests.exceptions.RetryError, requests.exceptions.SSLError, requests.exceptions.Timeout) as err:
            if hasattr(err, 'message'):
                print("Error(gtfs_s_req_class@request) msg:", err.message, "obj:", repr(err))
            else:
                print("Error(gtfs_s_req_class@request) obj:", repr(err))

            raise InternalConnectionError(message=f"GTFS_S_REQ_CLASS Error",
                                          error_obj=err, status_code=x.status_code).with_traceback(
                err.__traceback__) from err

    def get_gtfs_feed_data(self) -> dict:
        """

        This method `get_gtfs_feed_data` is responsible for retrieving GTFS feed data. It makes a request and returns the data along with the timestamp.

        Parameters:
        - None

        Returns:
        - A dictionary containing the GTFS feed data and the timestamp in UTC.

        Raises:
        - InternalConnectionError: If the request returns an invalid status code response.

        """
        response = self.request()

        if response["status"]:
            response_var = dict(data=response['data'], timestamp_utc=response["timestamp_utc"])
            return response_var
        else:

            raise InternalConnectionError("Invalid Status Code Response", response["status_code"],
                                          response)


class Prov_R_LinkInMemObj:
    """

    The Prov_R_LinkInMemObj class represents an in-memory storage object for S_Link data. It has the following methods:

    - __init__(max_elem): Initializes the Prov_R_LinkInMemObj object with the specified maximum number of elements.
    - process_data(obj_in) -> Union[list, None]: Static method that processes the input data and returns a processed list of dictionaries representing S_Link data. Returns None if the input is not in the expected format.
    - update(): Updates the Prov_R_LinkInMemObj object by retrieving data from the GTFS feed and storing it in the data store. Returns the updated data.
    - get_in_mem_last(): Returns the processed data for the last element in the data store.
    - get_buffer(): Returns the processed data for all elements in the data store.

    Example usage:

    link_obj = Prov_R_LinkInMemObj(10)
    link_obj.update()
    last_data = link_obj.get_in_mem_last()
    buffer_data = link_obj.get_buffer()

    """
    def __init__(self, max_elem):
        self.data_store = []
        self.elem_stored = 0
        self.request_obj = GTFS_S_RequestClass()

        self.max_elem = max_elem

    @staticmethod
    def process_data(obj_in:Union[dict,None]) -> Union[list, None]:
        """Process data.

        Process the given object and return a list of dictionaries containing the processed data.

        Args:
            obj_in (Union[dict, list]): The object to be processed. It can be a dictionary or a list.

        Returns:
            Union[list, None]: If the obj_in parameter is a dictionary, a list of dictionaries containing the processed data is returned. If the obj_in parameter is a list, a list of lists containing the processed data is returned. If the obj_in parameter is neither a dictionary nor a list, None is returned.

        """
        if obj_in is None:
            return None
        if 'decoded' in obj_in and 'timestamp_utc' in obj_in:
            work_obj = obj_in["decoded"]
            timestamp_utc = obj_in["timestamp_utc"]
            stream_array = list()
            for k in work_obj.entity:
                dict_obj = dict(id=k.id, timestamp=timestamp_utc)
                trip = dict(trip_id=k.vehicle.trip.trip_id)
                trip['route_id'] = k.vehicle.trip.route_id
                trip['direction_id'] = k.vehicle.trip.direction_id
                trip['start_time'] = k.vehicle.trip.start_time
                trip['schedule_relationship'] = k.vehicle.trip.schedule_relationship
                vehicle = dict()
                vehicle['id'] = k.vehicle.vehicle.id
                vehicle['license_plate'] = k.vehicle.vehicle.license_plate
                position = dict()
                position['latitude'] = k.vehicle.position.latitude
                position['longitude'] = k.vehicle.position.longitude
                position['timestamp'] = timestamp_utc
                dict_obj['vehicle'] = dict(trip=trip, vehicle=vehicle, position=position)
                stream_array.append(dict_obj)
            return stream_array
        elif isinstance(obj_in, list):
            array = []
            for k in obj_in:
                array.append(Prov_R_LinkInMemObj.process_data(k))
            return array
        else:
            return None

    def update(self):
        """

        The `update` method updates the data store with the latest GTFS feed data.

        Parameters:
        - None

        Returns:
        - data_var: A dictionary containing the raw feed data, the timestamp of the data, and the decoded feed message object.

        Exceptions:
        - ConnectionError: Raised when a connection error occurs while retrieving the GTFS feed data.
        - RuntimeError: Raised when an unknown exception occurs.

        """
        response_var = None
        try:
            response_var = self.request_obj.get_gtfs_feed_data()
            feed_msg_obj = gtfs_spec_pb2.FeedMessage()
            feed_size = feed_msg_obj.ParseFromString(response_var['data'])
            data_var = dict(raw=response_var['data'], timestamp_utc=response_var["timestamp_utc"],
                            decoded=feed_msg_obj)
            if feed_size > 0:

                if self.elem_stored >= self.max_elem:
                    self.data_store.pop(0)
                    self.data_store.append(data_var)
                else:
                    self.data_store.append(data_var)
                    self.elem_stored = self.elem_stored + 1
            else:
                print("feed size:", feed_size)
            return data_var

        except (InternalConnectionError, ConnectionError, ConnectionResetError, ConnectionRefusedError,
                ConnectionAbortedError) as err:
            # print(f"Connection error occurred:{repr(error)} \n at K:{self.request_obj.kontor}")
            raise ConnectionError(f"Connection error occurred:\n").with_traceback(err.__traceback__) from err
        except Exception as err:
            print(f"Unknown Exception occurred:{repr(err)} \n at K:{self.request_obj.kontor}")

            raise RuntimeError(f"Unknown Exception occurred:{repr(err)}").with_traceback(
                err.__traceback__) from err

    def get_in_mem_last(self):
        """
        Get the last element from the in-memory data store.

        Parameters:
        - self: The instance of the class.

        Returns:
        The result of processing the last element of the data store.
        """
        return self.process_data(self.data_store[-1])

    def get_buffer(self)->Union[list,None]:
        """
        Retrieves the buffer by processing the stored data.

        :return: The processed buffer.
        :rtype: Buffer object
        """
        return self.process_data(self.data_store)


class UpdateLinkSources(IntEnum):
    """
    Enum class representing the available link sources for updating.

    Each value corresponds to a specific link source, defined as an integer constant.

    Values:
    - R_API: Represents the  Provider API link source. Value is 0.
    - TH_API: Represents the Provider API link source. Value is 1.
    - PSG_API: Represents the PSG API link source. Value is 2.
    - S_AVL: Represents the Provider API link source. Value is 3.
    - Internal_SRV: Represents the Internal SRV link source. Value is 4.
    - Generic: Represents a generic link source. Value is 5.



    """
    R_API = 0
    TH_API = 1
    PSG_API = 2
    S_AVL = 3
    Internal_SRV = 4
    Generic = 5





class GenericGTFSItemClass:
    """
    A class to represent a generic GTFS item.

    Attributes:
        vehicle_capacity (int): The capacity of the vehicle.
        th_id (int): The th ID of the vehicle.
        r_prov_id (int): The r_prov ID of the vehicle.
        direction (int): The direction of the vehicle.
        route_id (int): The route ID of the vehicle.
        current_trip_id (int): The current trip ID of the vehicle.
        id (int): The vehicle ID.
        lic_plate (str): The license plate of the vehicle.
        pos_fifo (list): The position FIFO buffer.
        trip_fifo (list): The trip FIFO buffer.
        prop_buff_size (int): The property buffer size.
        pos_buff_size (int): The position buffer size.
        obj_lock (Lock): The lock for thread synchronization.
        record_timestamp (datetime): The record timestamp.
        trip_start_time (datetime): The start time of the trip.
        msg_id (int): The message ID.
        psg_fifo (list): The passenger FIFO buffer.
        last_record_update_src (int): The last record update source.
        velocity"""
    def __init__(self, vehicle_id: int, pos_buffer_size: int = 1024, prop_buffer_size:int = 1024):
        self.vehicle_capacity:Union[int,None] = None
        self.th_id = None
        self.r_prov_id = None
        self.direction = None
        self.route_id = None
        self.current_trip_id = None
        self.id = vehicle_id
        self.lic_plate = None
        self.pos_fifo = list()
        self.trip_fifo = list()
        self.prop_buff_size = prop_buffer_size
        self.pos_buff_size = pos_buffer_size
        self.obj_lock = Lock()
        self.record_timestamp = None
        self.trip_start_time = None
        self.msg_id = None
        self.psg_fifo = list()
        self.last_record_update_src = int(0)
        self.velocity_fifo = list()
        self.agencyId = None
        self.updated_since_last_output_flag = None

    def get_vehicle_capacity(self) -> Union[int,None]:
        """
        Get the capacity of the vehicle.

        :return: The capacity of the vehicle.
        :rtype: int
        """
        return self.vehicle_capacity

    def set_vehicle_capacity(self, capacity: int):
        """

        Set the capacity of the vehicle.

        Parameters:
            capacity (int): The new capacity of the vehicle.

        """
        if isinstance(capacity, int):
            self.vehicle_capacity = capacity
        else:
            self.vehicle_capacity = None

    def set_update_output_flag(self):
        """
        Sets the `updated_since_last_output_flag` attribute of the object to True.

        This method is used to indicate that the object has been updated since the last output.

        Parameters:
            self: The object instance.

        Returns:
            None
        """
        self.updated_since_last_output_flag = True

    def clear_update_output_flag(self):
        """
        Clears the flag indicating if updates have occurred since the last output.

        Parameters:
        - self: the instance of the class calling this method.

        Returns:
        - None
        """
        self.updated_since_last_output_flag = False

    def get_update_output_flag(self):
        """
        Returns the current value of the update output flag and resets it to False.

        If the update output flag is None, it is set to False.

        Returns:
            bool: The current value of the update output flag.

        """
        if self.updated_since_last_output_flag is None:
            self.updated_since_last_output_flag = False
            return True
        else:
            x = self.updated_since_last_output_flag
            self.updated_since_last_output_flag = False
            return x

    def set_agencyId(self, agencyId: int):
        """

        Set the agency ID.

        Sets the agency ID to the specified value.

        Parameters:
        - agencyId (int): The agency ID to be set.

        Raises:
        - ValueError: If the specified agency ID is not an integer.

        """
        if isinstance(agencyId, int):
            self.agencyId = agencyId
        else:
            self.agencyId = None
            raise ValueError("agencyId must be an integer")

    def get_agencyId(self) -> Union[int, None]:
        return self.agencyId

    def push_velocity(self, velocity: float, timestamp: datetime.datetime):
        if len(self.velocity_fifo) < self.prop_buff_size:
            self.velocity_fifo.append((velocity, timestamp))
        else:
            self.velocity_fifo.pop(0)
            self.velocity_fifo.append((velocity, timestamp))

    def get_velocity(self) -> Union[tuple[float, datetime.datetime], None]:
        if len(self.velocity_fifo) > 0:
            x = self.velocity_fifo[-1]
            if x[0] is not None and x[1] is not None:
                return x
            else:
                return None
        else:
            return None

    def get_velocity_fifo(self) -> list[tuple[float, datetime.datetime]]:
        return list(self.velocity_fifo)

    def get_current_psg_number(self) -> Union[int, None]:
        if len(self.psg_fifo) > 0:
            x = self.psg_fifo[-1]
            if x is not None:
                return int(x[0])
            else:
                return None
        else:
            return None

    def get_current_psg_timestamp(self) -> Union[datetime.datetime, None]:
        if len(self.psg_fifo) > 0:
            x = self.psg_fifo[-1]
            if x is not None:
                return x[1]
            else:
                return None
        else:
            return None

    def get_current_psg_tuple(self) -> Union[None, tuple[int, datetime.datetime, Union[int, None], Union[int, None]]]:
        if len(self.psg_fifo) > 0:
            x: tuple[int, datetime.datetime, int, int] = self.psg_fifo[-1]
            if x is not None:
                if x[0] is not None and x[1] is not None:
                    return x
                else:
                    return None
            else:
                return None
        else:
            return None

    def set_current_psg_number(self, current_psg_number: int, timestamp: datetime.datetime) -> Union[tuple[
        int, datetime.datetime], tuple[None, None]]:

        if len(self.psg_fifo) >= self.prop_buff_size:
            x: tuple[int, datetime.datetime] = self.psg_fifo.pop(0)
            if current_psg_number is not None and timestamp is not None:
                self.psg_fifo.append((current_psg_number, timestamp, None, None))
            else:
                self.psg_fifo.append((None, None, None, None))
            return x
        else:
            if current_psg_number is not None and timestamp is not None:
                self.psg_fifo.append((current_psg_number, timestamp, None, None))
            else:
                self.psg_fifo.append((None, None, None, None))
            return current_psg_number, timestamp

    def set_current_psg_number_extended(self, current_psg_number: int, timestamp: datetime.datetime, transit_in: int,
                                        transit_out: int) -> Union[tuple[
        int, datetime.datetime], tuple[None, None]]:

        if len(self.psg_fifo) >= self.prop_buff_size:
            x: tuple[int, datetime.datetime] = self.psg_fifo.pop(0)

            if current_psg_number is not None and timestamp is not None:
                self.psg_fifo.append((current_psg_number, timestamp, transit_in, transit_out))
            else:
                self.psg_fifo.append((None, None, None, None))
            return x
        else:
            if current_psg_number is not None and timestamp is not None:
                self.psg_fifo.append((current_psg_number, timestamp, transit_in, transit_out))
            else:
                self.psg_fifo.append((None, None, None, None))
            return current_psg_number, timestamp

    def get_psg_tuple_array_keep(self) -> Union[
        list[tuple[int, datetime.datetime, Union[int, None], Union[int, None]]], None]:
        if len(self.psg_fifo) > 0:
            return self.psg_fifo.copy()
        else:
            return None

    def get_psg_tuple_array_empty(self) -> Union[
        list[tuple[int, datetime.datetime, Union[int, None], Union[int, None]]], None]:
        if len(self.psg_fifo) > 0:
            x = self.psg_fifo.copy()
            self.psg_fifo.clear()
            return x
        else:
            return None

    def set_msg_id(self, msg_id: int):
        self.msg_id = msg_id

    def get_msg_id(self) -> Union[int, None]:
        return self.msg_id

    def set_trip_start_time(self, start_time: Union[datetime.datetime,None]):
        self.trip_start_time = start_time

    def get_trip_start_time(self) -> Union[None, datetime.datetime]:
        return self.trip_start_time

    def set_r_prov_id(self, r_prov_id: int):
        if isinstance(r_prov_id, int):
            self.r_prov_id = r_prov_id
        else:
            self.r_prov_id = hash(r_prov_id)

    def set_th_id(self, th_id: Union[int, str]):
        if isinstance(th_id, int):
            self.th_id = th_id
        else:
            self.th_id = hash(th_id)

    def set_id(self, vehicle_id: Union[int, Any]):
        if isinstance(vehicle_id, int):
            self.id = vehicle_id
        else:
            self.id = hash(vehicle_id)

    def get_r_prov_id(self) -> Union[int, None]:
        return self.r_prov_id

    def get_th_id(self) -> Union[int, None]:
        return self.th_id

    def get_id(self) -> Union[int, None]:
        return self.id

    def set_record_timestamp(self, timestamp: datetime.datetime, src: int):
        self.record_timestamp = timestamp
        self.last_record_update_src = src

    def get_record_timestamp(self) -> Union[datetime.datetime, None]:
        return self.record_timestamp

    def get_record_src(self) -> Union[int, None]:
        if self.record_timestamp is None:
            return None
        elif self.last_record_update_src is not None:
            return self.last_record_update_src
        else:
            return None

    def set_license_plate(self, plate_str: str):
        if isinstance(plate_str, str):
            self.lic_plate = plate_str
        else:
            self.lic_plate = str(plate_str)

    def get_license_plate(self) -> Union[str, None]:
        return self.lic_plate

    def push_pos(self, lat: float, long: float, timestamp: datetime.datetime):
        ret_val = (True, None)

        if len(self.pos_fifo) >= self.pos_buff_size:
            ret_val = (False, self.pos_fifo.pop(0))

        self.pos_fifo.append((lat, long, timestamp))


        return ret_val

    def get_pos_array_empty(self) -> list[tuple[float, float, datetime.datetime]]:
        pos_array = list(self.pos_fifo)
        self.pos_fifo.clear()
        return pos_array

    def get_pos_array_keep(self) -> list[tuple[float, float, datetime.datetime]]:

        # deep copy
        new_array = self.pos_fifo.copy()
        return new_array

    def get_pos(self) -> Union[tuple[float, float, datetime.datetime], None]:
        if len(self.pos_fifo) > 0:
            return self.pos_fifo[-1]
        else:
            return None

    def get_pos_by_index(self, index: int) -> Union[tuple[float, float, datetime.datetime], None]:
        if index < len(self.pos_fifo):
            return self.pos_fifo[index]
        else:
            return None

    def get_pos_fifo_size(self) -> int:
        return len(self.pos_fifo)

    def set_route_id(self, route_id: int):
        self.route_id = route_id

    def get_route_id(self) -> Union[int, None]:
        return self.route_id

    def clear_route_id(self):
        self.route_id = None

    def set_direction(self, direction: Union[bool,None]):
        self.direction = direction

    def get_direction(self) -> Union[bool, None]:
        return self.direction

    def set_current_trip_id(self, trip_id:Union[str,None]):
        if len(self.trip_fifo) < self.prop_buff_size:

            self.trip_fifo.append(trip_id)
            self.current_trip_id = trip_id
        else:

            self.trip_fifo.pop(0)
            self.trip_fifo.append(trip_id)
            self.current_trip_id = trip_id

    def get_current_trip_id(self) -> Union[str, None]:

        return self.current_trip_id


class ProviderInternalQuery:
    """
    A class that represents a ProviderInternalQuery for retrieving data from a specified IP address and port.

    Attributes:
        ip_address (IPAddress): The IP address to query.
        port (int): The port number to connect to.
        session (requests.Session): The requests session used for making HTTP requests.

    Methods:
        do_query() -> Union[dict, None]: Sends a GET request to the specified IP address and port and returns the response as a dictionary. Returns None if there is an error or the response is not valid JSON.
        process_data(obj) -> List[dict]: Processes the data received from the query response and returns the processed data as a list of dictionaries.
        do_query_proc() -> List[dict]: Sends a query and processes the received data using the `process_data` method.

    """
    def __init__(self, ip: str = "127.0.0.1", port: int = 8092, fifo_size: int = 10):
        self.ip_address = ipaddress.ip_address(ip)
        self.port = int(port)
        self.session = requests.Session()

    def do_query(self) -> Union[dict, None]:
        """
        Performs a query to a specified IP address and port.

        Parameters:
        - self: The current instance of the class.
        - Returns:
          - dict: A dictionary object containing the query result.
          - None: If there was an error during the query.

        Raises:
        - JSONDecodeError: If there is an error decoding the JSON response.
        - RequestException: If there is an exception during the request.

        The method sends a GET request to the specified IP address and port. If the request is successful
        (i.e., status code is 200), it attempts to decode the response content as JSON and returns the
        result as a dictionary object. If there is an error decoding the JSON response, it prints an error
        message and logs additional information if a logger is provided. In case of any exceptions during
        the request, it prints an error message and logs the exception if a logger is provided.

        Example usage:

        result = do_query()
        if result is not None:
             process the query result
        else:
             handle the error

        """
        global logger
        with self.session as session:
            try:
                request = session.request('GET', f'http://{self.ip_address}:{self.port}')

                if request.status_code == requests.codes.ok:
                    try:
                        data = request.content
                        obj = json.loads(data)
                        return obj
                    except JSONDecodeError as err:
                        print(f"JSON decode error:{err}")
                        if logger is not None:
                            logger.info("JSON Decode Error", exc_info=err, extra={'raw_data': data})
                        return None
            except requests.exceptions.RequestException as err:

                if logger is not None:
                    logger.info("Request exception", exc_info=err)
                return None
            except ConnectionError as e:

                if logger is not None:
                    logger.info("Connection exception", exc_info=err)
                return None

    @staticmethod
    def process_data(obj):
        """
        This method `process_data` takes in `obj` as a parameter and processes the data in the given `obj` to generate a stream array.

        If the `obj` is None, it prints a message indicating that the object is None and returns None.

        If the `obj` is not None, it iterates over the elements in the `obj` and performs the following operations:
        - Checks if the element has 'id' and 'vehicle' keys. If not, it continues to the next element.
        - Tries to convert the value of 'id' key to an integer. If it fails, it computes the hash value of the 'id' and assigns it to `msg_id`.
        - Extracts 'vehicle' object from the element.
        - If 'timestamp' key is present in the element, tries to convert its value to a datetime object in UTC timezone. If it fails, assigns the current datetime in UTC timezone to `timestamp_utc`.
        - Extracts the date part from `timestamp_utc`.
        - Creates a dictionary object `dict_obj` with 'id' as `msg_id` and 'timestamp' as `timestamp_utc`.
        - Creates a nested dictionary `trip` with 'trip_id' as "None".
        - If 'routeId' key is present in 'trip' object of 'vehicle', tries to convert its value to an integer. If it fails, computes the hash value of 'routeId' and assigns it to 'route_id' key in `trip`. If the key is not present, assigns `None` to 'route_id'.
        - If 'directionId' key is present in 'trip' object of 'vehicle', tries to convert its value to an integer. If it fails, assigns `None` to 'direction_id' in `trip`. If the key is not present, assigns `None` to 'direction_id'.
        - If 'startTime' key is present in 'trip' object of 'vehicle' and `date` is not None, converts the value to a datetime object in UTC timezone using the format '%H:%M:%S'. Assigns the resulting datetime object to 'start_time' key in `trip`. If the key is not present or `date` is None, assigns `None` to 'start_time'.
        - Assigns `None` to 'schedule_relationship' key in `trip`.
        - Creates a dictionary object `vehicle` with 'id' key. Tries to convert the value to an integer. If it fails, computes the hash value of 'id' and assigns it to 'id' key in `vehicle`. If the key is not present, assigns `None` to 'id'.
        - If 'licensePlate' key is present in 'vehicle' object of 'vehicle', assigns its value to 'license_plate' key in `vehicle`. If the key is not present, assigns `None` to 'license_plate'.
        - Creates a dictionary object `position` with 'latitude', 'longitude', and 'timestamp' keys. If 'position' key is present in 'vehicle', assigns its corresponding values to the respective keys in `position`. If the key is not present, assigns `None` to all keys.
        - Assigns `trip`, `vehicle`, and `position` dictionaries as values to 'vehicle' key in `dict_obj`.
        - Appends `dict_obj` to the `stream_array`.

        Finally, it returns the `stream_array` containing the processed data.

        Note: This method is a static method and does not require an instance of the class to be called.
        """
        if obj is None:
            print(f"Process data obj is None")
            return None

        else:
            stream_array = list()
            for i in obj:
                if 'id' not in i or 'vehicle' not in i:
                    continue
                try:
                    msg_id: int = int(i["id"])
                except ValueError as err:

                    msg_id: int = hash(i["id"])
                vehicle_obj = i["vehicle"]
                if 'timestamp' in vehicle_obj:
                    try:
                        timestamp_utc = datetime.datetime.fromtimestamp(vehicle_obj["timestamp"], tz=datetime.timezone.utc)
                    except ValueError as err:
                        timestamp_utc = datetime.datetime.now(tz=datetime.timezone.utc)

                    date = timestamp_utc.date()
                else:
                    timestamp_utc = datetime.datetime.now(tz=datetime.timezone.utc)
                    date = None
                dict_obj = dict(id=msg_id, timestamp=timestamp_utc)

                trip = dict(trip_id=str("None"))

                if 'routeId' in vehicle_obj["trip"]:
                    try:
                        trip['route_id'] = int(vehicle_obj["trip"]["routeId"])
                    except ValueError as err:
                        trip['route_id'] = hash(vehicle_obj["trip"]["routeId"])
                else:
                    trip['route_id'] = None

                if 'directionId' in vehicle_obj["trip"]:
                    try:
                        trip['direction_id'] = int(vehicle_obj["trip"]["directionId"])
                    except ValueError as err:
                        trip['direction_id'] = None
                else:
                    trip['direction_id'] = None

                if 'startTime' in vehicle_obj["trip"] and date is not None:
                    try:
                        trip['start_time'] = datetime.datetime.combine(date,
                                                                   datetime.datetime.strptime(
                                                                       vehicle_obj["trip"]["startTime"],
                                                                       "%H:%M:%S").time(),
                                                                   tzinfo=datetime.timezone.utc)
                    except (ValueError,KeyError) as err:
                        trip['start_time'] = None
                else:
                    trip['start_time'] = None

                trip['schedule_relationship'] = None
                vehicle = dict()

                try:
                    vehicle['id'] = int(vehicle_obj["vehicle"]["id"])
                except ValueError as err:
                    vehicle['id'] = hash(vehicle_obj["vehicle"]["id"])
                except KeyError as err:
                    vehicle['id'] = None
                if 'licensePlate' in vehicle_obj["vehicle"]:
                    vehicle['license_plate'] = vehicle_obj["vehicle"]["licensePlate"]
                else:
                    vehicle['license_plate'] = None
                position = dict()
                if 'position' in vehicle_obj:
                    position['latitude'] = vehicle_obj["position"]["latitude"]
                    position['longitude'] = vehicle_obj["position"]["longitude"]
                    position['timestamp'] = timestamp_utc
                else:
                    position['latitude'] = None
                    position['longitude'] = None
                    position['timestamp'] = None
                dict_obj['vehicle'] = dict(trip=trip, vehicle=vehicle, position=position)
                stream_array.append(dict_obj)

            return stream_array

    def do_query_proc(self):
        """
        Method Name: do_query_proc

        Description:
        This method executes a query and processes the returned data using the `process_data` method.

        Parameters:
            self: The instance of the class where this method is called.

        Returns:
            None

        Example Usage:
            obj = ClassName()
            obj.do_query_proc()
        """
        return self.process_data(self.do_query())


class MariaDBLinkClass:
    """

    Class MariaDBLinkClass represents a connection to a MariaDB database.

    Methods:
    - __init__(self, ip, port, user, passwd, db) : Constructor method that initializes the MariaDBLinkClass object with the given parameters.
    - exec_from_sql_file(self, file_path: str) -> bool : Executes SQL commands from a file.
    - connect(self) : Connects to the MariaDB database.
    - close(self) : Closes the connection to the database.
    - __del__(self) : Destructor method that automatically closes the connection when the object is deleted.
    - is_connected(self) -> bool : Checks if the connection to the database is active.
    - do_std_sql_transaction(self, statement, *argv) -> list : Executes a standard SQL transaction statement.

    Note: This class requires the `mariadb` module to be installed for establishing the connection.

    """
    def __init__(self, ip, port, user, passwd, db):
        self.connection = None
        self.ip = str(ip)
        self.username = str(user)
        self.password = str(passwd)
        self.port = int(port)
        self.queue_db = queue.Queue(maxsize=20)
        self.data_base = db
        self.connection_lock = threading.RLock()

    def __del__(self):
        if self.connection is not None:
            self.connection.close()
            del self.connection
        del self.ip
        del self.username
        del self.password
        del self.port
        try:
            while not self.queue_db.empty:

                self.queue_db.get_nowait()
                self.queue_db.task_done()
        except queue.Empty:
            pass
        del self.queue_db

        del self.data_base
        del self.connection_lock
    def exec_from_sql_file(self, file_path: str) -> bool:
        """

        Executes SQL commands from a file.

        Parameters:
        - file_path (str): The path to the SQL file to execute.

        Returns:
        - bool: True if the SQL commands were successfully executed, False otherwise.

        """
        global logger
        path_obj = pathlib.Path(file_path)
        if not path_obj.is_file():
            return False
        cursor = None
        ok = True
        with self.connection_lock:
            if self.connection is not None:
                try:
                    self.connection.begin()
                    cursor = self.connection.cursor(buffered=True)
                except mariadb.InterfaceError as err:
                    #print(f"MariDB Interface Error:{err}")
                    ok = False
                    if logger is not None:
                        logger.warning("MariaDB Interface Error", exc_info=err)
                    return False
                except mariadb.Error as err:

                    ok = False
                    raise RuntimeError_MariaDB(f"Exception occurred in MariaDB Module:{repr(err)}").with_traceback(
                        err.__traceback__) from err
                finally:
                    if cursor is not None:
                        if not cursor.closed:
                            cursor.close()
                        del cursor



                if path_obj.suffix == '.sql':
                    file_handle_read = open(str(path_obj), 'r', buffering=4096, encoding='utf-8')
                    file_content = file_handle_read.read()
                    file_handle_read.close()
                    sql_commands = file_content.split(';')
                    k = 0
                    ok = True
                    for command in sql_commands:
                        if command.strip():
                            try:
                                cursor.execute(command)
                                k = k + 1
                                if cursor.warnings != 0:
                                    if logger is not None:
                                        logger.info("Warnings:%s",str(self.connection.show_warnings()),exc_info=True)

                            except mariadb.ProgrammingError as err:
                                #print(f"MariDB Programming Error:{err}  \n  for command:{command} \n")
                                if logger is not None:
                                    logger.info("MariDB Programming Error", exc_info=err)
                                ok = False
                            except mariadb.InterfaceError as err:
                                #print(f"MariDB Interface Error:{err}")
                                if logger is not None:
                                    logger.warning("MariDB Interface Error", exc_info=err)
                                return False
                            except mariadb.Error as err:
                                #print(f"Grave Error occurred in  MariaDB: {err}")
                                raise RuntimeError_MariaDB(
                                    f"Exception occurred in MariaDB Module:{err}").with_traceback(
                                    err.__traceback__) from err
                            finally:
                                cursor.close()
                                del cursor

                    try:
                        if ok:
                            self.connection.commit()

                    except mariadb.InterfaceError as err:
                        #print(f"MariDB Interface Error:{err}")
                        if logger is not None:
                            logger.warning("MariDB Interface Error", exc_info=err)
                        return False
                    except mariadb.DatabaseError as err:
                        #print(f"MariDB DataBase Error:{err}")
                        if logger is not None:
                            logger.warning("MariDB Database Error", exc_info=err)
                        return False
                    except mariadb.Error as err:
                        #print(f"Grave Error occurred in  MariaDB: {err}")
                        raise RuntimeError_MariaDB(
                            f"Exception occurred in MariaDB Module:{err}").with_traceback(
                            err.__traceback__) from err
                    return ok

                else:
                    return False

    def connect(self):
        """
        The `connect` method is used to establish a connection with a MariaDB database. It takes no parameters and returns a `Connection` object.

        If a connection has not been established yet, the method attempts to connect to the database using the specified host, port, username, password, and connection timeout. If a database name is provided, it also selects that database.

        If the connection is successful, log messages containing server information and capabilities are logged, and the `Connection` object is returned.

        If an `InterfaceError` occurs during the connection attempt, a warning log message is logged, the `Connection` object is set to `None`, and `None` is returned.

        If any other `Error` occurs during the connection attempt, a `RuntimeError_MariaDB` exception is raised with a customized error message and the original traceback.

        If a connection already exists, log messages are logged indicating that the module is already connected and reconnecting. The `close` method is called to close the existing connection, and the `connect` method is recursively called to establish a new connection.

        """
        global logger
        with self.connection_lock:
            if self.connection is None:
                try:
                    self.connection = mariadb.connect(host=self.ip, port=self.port, user=self.username,
                                                      password=self.password, connect_timeout=5, reconnect=False)
                    if self.data_base is not None:
                        self.connection.select_db(str(self.data_base))
                    if logger is not None:
                        logger.info(
                        f"MariaDB connection established\nServer Version:{self.connection.server_version_info} \nServer "
                        f"Info:{self.connection.server_info} \n"
                        f"Server Capabilities:{self.connection.server_capabilities}\n"
                        f"Database:{self.connection.database}\n")

                    return self.connection
                except mariadb.InterfaceError as err:
                    #print(f"MariDB encoutered Inteface Error while connecting:{err}")
                    if logger is not None:
                        logger.warning("MariDB Interface Error", exc_info=err)
                    self.connection = None
                    return None
                except mariadb.Error as err:
                    #print(f"Error connecting to the database: {err}")
                    self.connection = None
                    raise RuntimeError_MariaDB(
                        f"MariaDB module encountered a connection error to ip:{self.ip},port:{self.port}\n").with_traceback(
                        err.__traceback__) from err
            else:
                if logger is not None:
                    logger.info("MariaDB module already connected.Reconnecting...\n")
                if logger is not None:
                    logger.info("MariaDB module already connected.Reconnecting...\n")
                self.close()
                return self.connect()

    def close(self):
        """
        Closes the database connection and commits any pending changes.

        Raises:
            RuntimeErrorMariaDB: If a grave error occurs in the MariaDB module.

        """
        try:
            with self.connection_lock:
                if self.connection is not None:
                    self.connection.close()
                    self.connection = None
        except (mariadb.ProgrammingError, mariadb.InterfaceError) as err:
            #print(f"Error in MariaDB: {repr(err)}")
            if logger is not None:
                logger.warning("Programming/Interface Error on close.", exc_info=err)
            self.connection = None

        except mariadb.Error as err:
            #print(f"Grave Error occurred in  MariaDB: {repr(err)}")
            # self.connection.rollback()
            self.connection = None
            raise RuntimeError_MariaDB(f"Exception occurred in MariaDB Module:{repr(err)}").with_traceback(
                err.__traceback__) from err



    def is_connected(self) -> bool:
        """
        Check if the connection to the MariDB server is active.

        Returns:
            bool: True if the connection is active, False otherwise.
        """
        with self.connection_lock:
            if self.connection is not None:
                val = True
                try:
                    self.connection.ping()
                    val = True
                except mariadb.InterfaceError as error:
                    val = False
                    if logger is not None:
                        logger.info("MariDB Interface Error(lost connection?)", exc_info=error)
                    #print(f"MariaDB Lost Connection:{error}")
                    self.close()
                return val
            else:
                return False

    def do_std_sql_transaction(self, statement, *argv) -> list:
        """
        Do a standard SQL transaction with the given statement and arguments.

        Parameters:
            statement (str): The SQL statement to execute.
            *argv: Optional arguments to be injected into the SQL statement.

        Returns:
            list: List of results fetched from the executed SQL statement.

        Raises:
            RuntimeErrorMariaDB: If the MariaDB module encounters an error during the SQL transaction.

        Note:
            This method requires an active database connection. If there is no active connection, an empty list will be returned.

        Example:
            # Create an instance of the class
            db = Database()

            # Connect to the database
            db.connect()

            # Execute a SQL statement with arguments
            result = db.do_std_sql_transaction("SELECT * FROM users WHERE age > %s", 18)

            # Print the result
            print(result)
        """
        global logger
        arg_sql = [arg for arg in argv]
        return_val = []
        if self.connection is not None:
            try:
                self.connection.begin()
                cursor = self.connection.cursor()
                cursor.execute(statement, arg_sql, buffered=True)
                if cursor.rowcount > 0:
                    tmp = cursor.fetchone()
                    return_val = []
                    while tmp is not None:
                        return_val.append(tmp)
                        tmp = cursor.fetchone()
                cursor.close()
                self.connection.commit()
                return return_val
            except mariadb.Error as err:
                raise RuntimeError_MariaDB(
                    f'MariaDB module encountered an error during SQL transaction:{err}').with_traceback(
                    err.__traceback__)
        else:
            return []


def compare_license_strings(s1, s2):
    """
    Compare two license strings and return True if they are equal after removing special characters and spaces.

    Parameters:
    - s1 (str): The first license string to compare.
    - s2 (str): The second license string to compare.

    Returns:
    - bool: True if the license strings are equal, False otherwise.

    Example:
        s1 = "MIT License"
        s2 = "MIT-License"
        result = compare_license_strings(s1, s2)
        print(result)
        # Output: True
    """
    s1 = s1.replace("-", "").replace(".", "").replace("/", "").replace(" ", "")
    s2 = s2.replace("-", "").replace(".", "").replace("/", "").replace(" ", "")
    return s1.lower() == s2.lower()


class SqlGtfsDatabase:
    """
    SqlGtfsDatabase is a class that represents a connection to a GTFS database.

    Attributes:
        lock (RLock): A reentrant lock for thread safety.
        db_table_list_timestamp (None): A timestamp indicating the last time the database table list was updated.
        db_table_list (None): A list of tables in the database.
        current_vehicle_pos_table (None): The name of the table containing current vehicle position data.
        current_vehicle_psg_info_table (None): The name of the table containing current vehicle passenger information.
        connection (None): The database connection object.
        mariadb_object (MariaDBLinkClass): An object representing the MariaDB connection.
        vehicle_list (None): A dictionary containing vehicle information.
        vehicle_correlation_dict_lp (dict): A dictionary mapping license plates to th park IDs.
        vehicle_correlation_list_th (dict): A dictionary mapping th park IDs to license plates.
        timestamp_db_vehicles_table (None): A timestamp indicating the last time the vehicle table was updated.
        update_vehicles_table_interval (int): The interval in seconds for updating the vehicle table.

    Methods:
        connect(): Establishes a connection to the database.

        get_vehicle_dict() -> Union[dict, None]: Retrieves the vehicle dictionary from the database.

        get_vehicle_correlation_data() -> Union[tuple[dict, dict], None]: Retrieves the vehicle correlation data from the database.

        is_connected(): Checks whether the database connection is established.

        update_vehicle_table(data) -> bool: Updates the vehicle table in the database.

    """
    def __init__(self, ip: str, port: int, user: str, passwd: str, db: str):
        self.lock = RLock()
        self.db_table_list_timestamp = None
        self.db_table_list = None
        self.current_vehicle_pos_table = None
        self.current_vehicle_psg_info_table = None
        self.connection = None
        self.mariadb_object = MariaDBLinkClass(str(ip), int(port), str(user), str(passwd), str(db))
        self.vehicle_list = None
        self.vehicle_correlation_dict_lp = dict()
        self.vehicle_correlation_list_th = dict()
        # self.queue_db = queue_var.Queue(maxsize=20)
        self.timestamp_db_vehicles_table = None
        self.update_vehicles_table_interval = 3600  # seconds

    def __del__(self):
        with self.lock:
            self.mariadb_object.close()
            del self.connection

            del self.update_vehicles_table_interval
            del self.timestamp_db_vehicles_table
            del self.vehicle_correlation_dict_lp
            del self.vehicle_correlation_list_th
            del self.vehicle_list
            del self.current_vehicle_psg_info_table
            del self.current_vehicle_pos_table
            del self.db_table_list
            del self.db_table_list_timestamp
            del self.mariadb_object
        del self.lock

    def connect(self):
        with self.lock:
            self.connection = self.mariadb_object.connect()
        return self.connection

    def get_vehicle_dict(self) -> Union[dict, None]:
        with self.lock:
            if self.timestamp_db_vehicles_table is not None:
                return self.vehicle_list
            else:
                return None

    def get_vehicle_correlation_data(self) -> Union[tuple[dict, dict], None]:
        global logger

        statement_query_license_plate_corr = "SELECT th_park_id,license_plate,passenger_capacity FROM gtfs_collection.License_Plate_Correlation"
        with self.lock:
            if self.connection is None:
                return None
            else:
                try:
                    self.connection.begin()
                    with self.connection.cursor(buffered=True) as cursor:
                        cursor.execute(statement_query_license_plate_corr)
                        mariadb_result = cursor.fetchone()
                        vehicle_list_data_corr = list()
                        while mariadb_result is not None:
                            tmp = [item for item in mariadb_result]
                            vehicle_list_data_corr.append(tmp)
                            mariadb_result = cursor.fetchone()
                    # Execute from SQL File
                    # self.mariadb_object.exec_from_sql_file('./database/query_db_modify.sql')
                    with self.lock:
                        self.vehicle_correlation_dict_lp.clear()
                        self.vehicle_correlation_list_th.clear()
                        for i in vehicle_list_data_corr:
                            if i[0] is not None and i[1] is not None and i[2] is not None:
                                # update license plate
                                license_plate = str(i[1])
                                try:
                                    th_id = int(i[0])
                                except ValueError:
                                    th_id = hash(i[0])

                                self.vehicle_correlation_dict_lp[license_plate] = th_id
                                self.vehicle_correlation_list_th[th_id] = license_plate
                    self.connection.commit()

                    with self.lock:
                        ret_obj = copy.deepcopy((self.vehicle_correlation_dict_lp, self.vehicle_correlation_list_th))
                    return ret_obj
                except mariadb.InterfaceError as err:
                    if logger is not None:
                        logger.warning("MariaDB Interface Error:", exc_info=err)
                    # self.vehicle_correlation_dict_lp.clear()
                    # self.vehicle_correlation_list_th.clear()
                    return None
                except mariadb.ProgrammingError as err:
                    if logger is not None:
                        logger.info("MariaDB Programming Error:", exc_info=err)
                    # self.vehicle_correlation_dict_lp.clear()
                    # self.vehicle_correlation_list_th.clear()
                    return None
                except mariadb.DatabaseError as err:
                    if logger is not None:
                        logger.warning("MariaDB Database Error:", exc_info=err)
                    # self.vehicle_correlation_dict_lp.clear()

                    return None
                except mariadb.Error as err:
                    # self.vehicle_correlation_dict_lp.clear()
                    raise RuntimeError_MariaDB("MariaDB Error") from err

    def is_connected(self):
        with self.lock:
            return self.mariadb_object.is_connected()

    # Refresh the DataBase Vehicle Table
    # Time gated run
    def update_vehicle_table(self, data) -> bool:
        with self.lock:
            run = False
            timestamp_vehicles_table = self.timestamp_db_vehicles_table
            now = datetime.datetime.now(datetime.timezone.utc)
            if timestamp_vehicles_table is None:
                run = True

            elif (now - timestamp_vehicles_table).total_seconds() >= self.update_vehicles_table_interval:
                run = True
            statement_query_table_data = "SELECT vehicle_db_id,vehicle_r_prov_app_id,vehicle_th_app_id,vehicle_license_plate_nr FROM Vehicles"
            statement_insert_new_vehicle_id = (
                "INSERT INTO gtfs_collection.Vehicles (vehicle_r_prov_app_id,vehicle_th_app_id,"
                "vehicle_license_plate_nr) VALUES (%d,%d,%s) ON DUPLICATE KEY UPDATE vehicle_r_prov_app_id=%d,"
                "vehicle_th_app_id=%d,vehicle_license_plate_nr=%s")
            if run:
                vehicle_correlation = self.get_vehicle_correlation_data()
                if vehicle_correlation is None:
                    return False
                if self.connection is not None:
                    cursor = None
                    try:
                        self.connection.begin()
                        cursor = self.connection.cursor(buffered=True)
                        upload_data_list = []

                        if isinstance(data, list):

                            if len(data) > 0:

                                for element in data:
                                    # parse each item in list
                                    if isinstance(element, dict):

                                        vehicle_r_prov_app_id = None
                                        vehicle_th_app_id = None
                                        license_plate = None
                                        if 'vehicle' in element:
                                            if 'vehicle' in element['vehicle']:
                                                if 'license_plate' in element['vehicle']['vehicle']:
                                                    license_plate = element['vehicle']['vehicle']['license_plate']
                                                if 'id' in element['vehicle']['vehicle'] and 'src' in element:
                                                    # Look at Update source to determine if using th or r_prov IDs
                                                    if element['src'] is not None:
                                                        if element['src'] == UpdateLinkSources.R_API.value or \
                                                                element[
                                                                    'src'] == UpdateLinkSources.S_AVL.value:
                                                            vehicle_r_prov_app_id = int(
                                                                element['vehicle']['vehicle']['id'])
                                                        if element['src'] == UpdateLinkSources.TH_API.value or \
                                                                element[
                                                                    'src'] == UpdateLinkSources.PSG_API.value:
                                                            vehicle_th_app_id = int(
                                                                element['vehicle']['vehicle']['id'])
                                                        if element['src'] == UpdateLinkSources.Internal_SRV.value:
                                                            vehicle_r_prov_app_id = int(
                                                                element['vehicle']['vehicle']['id'])
                                        if vehicle_r_prov_app_id is not None and license_plate is not None and vehicle_th_app_id is None:
                                            lp_key = str(license_plate)
                                            if lp_key in vehicle_correlation[0]:
                                                vehicle_th_app_id = int(vehicle_correlation[0][lp_key])

                                            upload_data = (vehicle_r_prov_app_id, vehicle_th_app_id, license_plate,
                                                           vehicle_r_prov_app_id, vehicle_th_app_id, license_plate)
                                            upload_data_list.append(upload_data)

                                        else:
                                            continue
                                if len(upload_data_list) > 0:
                                    cursor.executemany(statement_insert_new_vehicle_id, upload_data_list)
                                    self.timestamp_db_vehicles_table = now

                                """   
                                Not thread safe         
                                with warnings.catch_warnings(record=True) as w:
    
    
                                if logger is not None:
                                    if len(w) > 0:
                                        for i in w:
                                            logger.info("MariaDB Warning:%s Type:%s Line:%s Source:%s", str(i.message),
                                                        str(i.category), str(i.source), exc_info=True)
                                """
                        cursor.execute(statement_query_table_data)
                        mariadb_result = cursor.fetchone()
                        with self.lock:
                            if self.vehicle_list is not None:
                                self.vehicle_list.clear()
                            else:
                                self.vehicle_list = dict()

                            while mariadb_result is not None:
                                if mariadb_result[0] is not None:
                                    try:
                                        key_db = int(mariadb_result[0])
                                    except ValueError:
                                        key_db = hash(str(mariadb_result[0]))
                                    self.vehicle_list[key_db] = [item for item in mariadb_result]
                                if mariadb_result[3] is not None:
                                    try:
                                        key_lp = str(mariadb_result[3])
                                    except ValueError:
                                        key_lp = hash(mariadb_result[3])
                                    self.vehicle_list[key_lp] = [item for item in mariadb_result]
                                mariadb_result = cursor.fetchone()


                    except mariadb.InterfaceError as err:
                        if logger is not None:
                            logger.warning(f"MariDB Interface Error", exc_info=err)
                        return False
                    except mariadb.ProgrammingError as err:

                        self.timestamp_db_vehicles_table = None
                        if isinstance(err, mariadb.ProgrammingError):
                            self.connection.rollback()

                        if logger is not None:
                            logger.info("MariDB ProgrammingError", exc_info=err)
                        return False

                    except mariadb.Error as err:
                        print(f"Grave Error occurred in  MariaDB: {repr(err)}")
                        # self.connection.rollback()
                        raise RuntimeError_MariaDB(f"Exception occurred in MariaDB Module") from err
                    finally:
                        if cursor is not None:
                            if not cursor.closed:
                                cursor.close()
                            del cursor
                else:
                    if logger is not None:
                        logger.info(f"MariaDB connection is None")

                    return False
            else:
                return True

    @staticmethod
    def table_name_template_factory_vehicle_positions(m, y):
        return f"Vehicle_Positions_{str(m)}_{str(y)}"

    @staticmethod
    def match_table_to_regex(m, y, str_in):
        if str_in is None:
            return False
        match = re.match(r'Vehicle_Positions_(\d{1,2})_(\d{4})', str_in)
        try:
            m_match = int((match.group(1)))
            y_match = int((match.group(2)))
        except (ValueError, KeyError) as err:
            return False
        return True if m_match == m and y_match == y else False

    def update_vehicle_position_table(self, obj_data: list) -> int:
        global logger
        with self.lock:
            now = datetime.datetime.now(tz=datetime.timezone.utc)

            month = now.date().month
            year = now.date().year
            available_table_names_query = "SHOW TABLES;"

            template_search_regex = re.compile(r'Vehicle_Positions_(\d{1,2})_(\d{4})')

            if self.connection is not None:
                cursor = None
                try:
                    self.connection.begin()
                    cursor = self.connection.cursor()

                    if self.db_table_list is None or self.db_table_list_timestamp is None:
                        cursor.execute(available_table_names_query)
                        tables_tmp = cursor.fetchall()
                        tables = list()
                        for i in tables_tmp:
                            tables.append(i[0])
                        self.db_table_list_timestamp = now
                        self.db_table_list = tables
                    elif (now - self.db_table_list_timestamp) >= datetime.timedelta(
                            days=1):
                        cursor.execute(available_table_names_query)
                        tables_tmp = cursor.fetchall()
                        tables = list()
                        for i in tables_tmp:
                            tables.append(i[0])
                        self.db_table_list_timestamp = now
                        self.db_table_list = tables
                    else:
                        tables = self.db_table_list
                except mariadb.ProgrammingError as err:
                    if logger is not None:
                        logger.info(f"MariaDB Error on QUERY", exc_info=err)
                    return 2
                except mariadb.InterfaceError as err:
                    if logger is not None:
                        logger.warning(f"Interface Error occurred in  MariaDB", exc_info=err)
                    return 3
                except mariadb.Error as err:

                    raise RuntimeError_MariaDB(f"Exception occurred in MariaDB Module") from err
                match_list = []
                create_table = not self.match_table_to_regex(month, year, self.current_vehicle_pos_table)

                for i in tables:
                    match = template_search_regex.match(i)
                    if match is not None:
                        match_list.append(match)
                if len(match_list) > 0:
                    for i in match_list:
                        match_str = i.group(0)
                        match = re.match(match_str, self.table_name_template_factory_vehicle_positions(month, year))
                        if match:
                            create_table = False
                            self.current_vehicle_pos_table = match.group(0)
                            break
                if create_table:

                    table_name = self.table_name_template_factory_vehicle_positions(month, year)
                    print(f"Create New table:{table_name}")
                    create_table_query = (f"CREATE TABLE IF NOT EXISTS {table_name} ("
                                          f"vehicle_db_id int unsigned null,"
                                          f"position_db_id bigint unsigned auto_increment primary key,"
                                          f"position_long float not null,"
                                          f"position_lat  float not null,"
                                          f"mariadb_point point null,"
                                          f"time_stamp_location_info timestamp(4) default current_timestamp(4) not null,"
                                          f"acquisition_mode varchar(16) null,"
                                          f"constraint FK_TYPE_VEHICLE_DB_ID_{str(month)}_{str(year)} foreign key (vehicle_db_id) references "
                                          f"Vehicles (vehicle_db_id) on update cascade on delete cascade)")
                    index_query = f"CREATE or REPLACE index INDEX_VEHICLE_DB on {table_name} (vehicle_db_id);"
                    create_trigger_insert = (
                        f"create  trigger gtfs_collection.Insert_MARIADB_point_{month}_{year} "
                        f"before insert on gtfs_collection.{table_name} "
                        f"for each row "
                        f"BEGIN "
                        f"SET NEW.mariadb_point = (IF(NEW.mariadb_point is NULL,point(NEW.position_long, "
                        f"NEW.position_lat), NEW.mariadb_point));"
                        f"END;")
                    create_trigger_update = (
                        f"create  trigger gtfs_collection.Update_MARIADB_point_{month}_{year} "
                        f"before update on gtfs_collection.{table_name} "
                        f"for each row "
                        f"BEGIN "
                        f"SET NEW.mariadb_point = (IF(NEW.mariadb_point is NULL,point("
                        f"NEW.position_long,"
                        f"NEW.position_lat), NEW.mariadb_point));"
                        f"END;")

                    success = False
                    try:
                        cursor.execute(create_table_query)
                        cursor.execute(index_query)
                        cursor.execute(create_trigger_insert)
                        cursor.execute(create_trigger_update)
                        self.connection.commit()
                        # cursor.close()
                        success = True
                    except mariadb.ProgrammingError as err:
                        if logger is not None:
                            logger.info(f"MariaDB Error on INSERT", exc_info=err)
                        return 2
                    except mariadb.InterfaceError as err:
                        if logger is not None:
                            logger.info(f"Interface Error occurred in  MariaDB", exc_info=err)
                        return 3
                    except mariadb.Error as err:
                        # print(f"Grave Error occurred in  MariaDB: {repr(err)}")
                        # self.connection.rollback()
                        raise RuntimeError_MariaDB(f"Exception occurred in MariaDB Module") from err
                    finally:
                        if cursor is not None:
                            if not cursor.closed:
                                cursor.close()
                            del cursor
                    if not success:
                        return 1

                    self.current_vehicle_pos_table = table_name


                ret_val = self.write_vehicle_position_data(obj_data, self.current_vehicle_pos_table)
                if ret_val != 0:
                    return 3
                return 0

    def write_vehicle_position_data(self, obj_output: list, table_name: str) -> int:
        global logger
        with self.lock:
            exit_status = 0

            statement_upload_positions = (f"INSERT INTO gtfs_collection.{table_name} (vehicle_db_id,"
                                          "position_long,position_lat,time_stamp_location_info,"
                                          "acquisition_mode) VALUES (%d,%s,%s,%s,%s);")
            if self.connection is not None:

                k = 0
                cursor = None
                try:
                    self.connection.begin()
                    cursor = self.connection.cursor(buffered=True)
                    db_upload_array = []
                    for i in obj_output:
                        # %d,%f,%f,%s,%s
                        lat = i['vehicle']['position']['latitude']
                        long = i['vehicle']['position']['longitude']
                        timestamp_pos = i['vehicle']['position']['timestamp']
                        app_id = i['vehicle']['vehicle']['id']
                        src = i['src']
                        l_plate = i['vehicle']['vehicle']['license_plate']

                        if lat is None or src is None or long is None or app_id is None:
                            k = k + 1
                            continue
                        lat = float(lat)
                        long = float(long)
                        src = int(src)
                        app_id = int(app_id)
                        db_id = None
                        aq_mode = None
                        if src == UpdateLinkSources.S_AVL.value or src == UpdateLinkSources.R_API or src == UpdateLinkSources.Internal_SRV:
                            tmp = None
                            if l_plate is not None and str(l_plate) in self.vehicle_list:
                                tmp = self.vehicle_list[str(l_plate)]
                            else:
                                for key, val in self.vehicle_list.items():
                                    if val[1] is not None:
                                        if val[1] == app_id:
                                            tmp = val
                                            break
                            if tmp is not None:
                                db_id = int(tmp[0])
                                aq_mode = "r_prov"
                        elif src == UpdateLinkSources.TH_API.value:
                            tmp = None
                            if l_plate is not None and str(l_plate) in self.vehicle_list:
                                tmp = self.vehicle_list[str(l_plate)]
                            else:
                                for key, val in self.vehicle_list.items():
                                    if val[2] is not None:
                                        if val[2] == app_id:
                                            tmp = val
                                            break
                            if tmp is not None:
                                db_id = int(tmp[0])
                                aq_mode = "th"

                        if db_id is None:
                            k = k + 1
                            continue
                        timestamp_db = None
                        if timestamp_pos is None:
                            timestamp_pos = i['timestamp']

                        if isinstance(timestamp_pos, datetime.datetime):
                            timestamp_db = timestamp_pos.strftime('%Y-%m-%d %H:%M:%S')

                        else:
                            k = k + 1
                            continue

                        tuple_upload = (db_id, long, lat, timestamp_db, aq_mode)
                        db_upload_array.append(tuple_upload)
                    if len(db_upload_array) > 0:
                        cursor.executemany(statement_upload_positions, db_upload_array)

                    self.connection.commit()

                    cursor.close()
                    if k > 0:
                        exit_status = 2
                    return exit_status
                except mariadb.ProgrammingError as err:
                    if logger is not None:
                        logger.info(f"Error querying/inserting data to MariaDB", exc_info=err)

                    self.connection.rollback()
                    exit_status = 1
                    return exit_status
                except mariadb.InterfaceError as err:
                    if logger is not None:
                        logger.warning(f"Interface Error occurred in  MariaDB", exc_info=err)
                    exit_status = 3
                    return exit_status
                except mariadb.Error as err:
                    raise RuntimeError_MariaDB(f"Exception occurred in MariaDB Module") from err
                finally:
                    if cursor is not None:
                        if not cursor.closed:
                            cursor.close()
                        del cursor
            else:
                exit_status = 1
                return exit_status

    def write_passenger_data(self, obj_output: list, table_name: str) -> int:
        global logger
        exit_status = 0
        with self.lock:
            if self.connection is not None:
                cursor = None
                try:
                    self.connection.begin()
                    cursor = self.connection.cursor()

                    k = 0
                    db_upload_array = []

                    for i in obj_output:
                        psg_count = i['passenger_info']['on_board']
                        timestamp = i['passenger_info']['timestamp']
                        app_id = i['vehicle']['vehicle']['id']
                        src = i['src']
                        vehicle_db_id = None
                        if psg_count is None or timestamp is None or src is None or app_id is None:
                            continue
                        if not isinstance(timestamp, datetime.datetime):
                            continue
                        psg_count = int(psg_count)
                        if src == UpdateLinkSources.S_AVL.value or src == UpdateLinkSources.R_API or src == UpdateLinkSources.Internal_SRV:
                            dict_tmp = self.vehicle_list['vehicle_r_prov_app_id']
                            if app_id in dict_tmp:
                                vehicle_db_id = int(dict_tmp[app_id][0])

                        elif src == UpdateLinkSources.TH_API.value or src == UpdateLinkSources.PSG_API.value:
                            dict_tmp = self.vehicle_list['vehicle_th_app_id']
                            if app_id in dict_tmp:
                                vehicle_db_id = int(dict_tmp[app_id][0])

                        if vehicle_db_id is None:
                            k = k + 1
                            continue

                        timestamp_db = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                        tuple_upload = (vehicle_db_id, psg_count, timestamp_db)
                        db_upload_array.append(tuple_upload)
                except mariadb.ProgrammingError as err:
                    if logger is not None:
                        logger.info(f"Error querying/inserting data to MariaDB", exc_info=err)

                    self.connection.rollback()
                    exit_status = 1
                    return exit_status
                except mariadb.InterfaceError as err:
                    if logger is not None:
                        logger.warning(f"Interface Error occurred in  MariaDB", exc_info=err)
                    exit_status = 3
                    return exit_status
                except mariadb.Error as err:
                    raise RuntimeError_MariaDB(f"Exception occurred in MariaDB Module") from err
                finally:
                    if cursor is not None:
                        if not cursor.closed:
                            cursor.close()
                        del cursor


##  Global Class the handles the encapsulation of all GTFS Vehicle Objects
#   The dict is indexed by string of str(vehicle_id:int)
#   The Class supports output generation/parsing for HTTP export

class VehicleMemoryDB:
    """
    Class representing a Vehicle Memory Database.

    Attributes:
    - serial_out_cache: The serial output cache.
    - obj_lock: The RLock object used for locking access to the database.
    - output_cache_timestamp: The timestamp of the output cache.
    - vehicle_dict_r_prov: The dictionary containing vehicles from r_prov source.
    - vehicle_dict_th: The dictionary containing vehicles from th source.
    - lock_http_thread: The threading condition object for synchronization.
    - pos_buffer_size: The size of the position buffer.
    - prop_buffer_size: The size of the property buffer.
    - id_maps: The list of dictionaries mapping IDs between different sources.
    - correlation_tuple: The tuple containing correlation data.
    - sql_obj: The SqlGtfsDatabase object used for SQL operations.

    Methods:
    - update_correlation_dict: Update the correlation dictionary with data from the SQL object.
    - get_id_map_db: Get the ID mapping from the database source.
    - get_id_map_th: Get the ID mapping from the th source.
    - get_id_map_r_prov: Get the ID mapping from the r_prov source.
    - create_map: Create a mapping between r_prov, th, and database IDs.
    - create_vehicle: Create a vehicle object and store it in the appropriate dictionary.
    - get_vehicle: Get a vehicle object from the appropriate dictionary based on the ID and source.

    Note: This class utilizes the @lru_cache decorator to cache the results of the create_vehicle and get_vehicle methods.
    """
    def __init__(self, pos_buffer_size, prop_buffer_size:int, sql_obj: SqlGtfsDatabase):
        self.serial_out_cache = list()
        self.obj_lock = RLock()

        self.output_cache_timestamp = None
        self.vehicle_dict_r_prov = dict()
        self.vehicle_dict_th = dict()
        self.lock_http_thread = threading.RLock()
        self.pos_buffer_size = pos_buffer_size
        self.prop_buffer_size = prop_buffer_size
        self.id_maps = [dict(), dict(), dict()]
        self.correlation_tuple: Union[None, tuple[dict, dict]] = None
        self.sql_obj = sql_obj

    def update_correlation_dict(self):
        """
        Updates the correlation dictionary.

        This method retrieves the vehicle correlation data from the SQL object and updates the correlation dictionary with it. The correlation dictionary is stored in the instance variable 'correlation_tuple'.

        Parameters:
            None

        Returns:
            None
        """
        data = self.sql_obj.get_vehicle_correlation_data()
        if data is not None:
            with self.obj_lock:
                self.correlation_tuple = copy.deepcopy(data)
            del data

    def get_id_map_db(self, id_db: any) -> Union[tuple[int, int], None]:
        if id_db is None:
            return None
        if not isinstance(id_db, int):
            dict_key_db = hash(id_db)
        else:
            dict_key_db = id_db

        if dict_key_db not in self.id_maps[1]:
            return None
        else:
            return self.id_maps[2][dict_key_db]

    def get_id_map_th(self, id_th: any) -> Union[tuple[int, int], tuple[int, None], None]:
        if id_th is None:
            return None
        if not isinstance(id_th, int):
            if isinstance(id_th, str):
                try:
                    dict_key_th = int(id_th)
                except ValueError as err:
                    dict_key_th = hash(id_th)
            else:
                dict_key_th = hash(id_th)
        else:
            dict_key_th = id_th
            if dict_key_th in self.id_maps[1]:
                return self.id_maps[1][dict_key_th]
            else:
                return None

    def get_id_map_r_prov(self, id_r_prov: any) -> Union[tuple[int, int], tuple[int, None], None]:
        if id_r_prov is None:
            return None
        if not isinstance(id_r_prov, int):
            if isinstance(id_r_prov, str):
                try:
                    dict_key_r_prov = int(id_r_prov)
                except ValueError as err:
                    dict_key_r_prov = hash(id_r_prov)
            else:
                dict_key_r_prov = hash(id_r_prov)
        else:
            dict_key_r_prov = id_r_prov
            if dict_key_r_prov in self.id_maps[0]:
                return self.id_maps[0][dict_key_r_prov]
            else:
                return None

    def create_map(self, id_r_prov: int, id_th: int, id_db: int) -> bool:
        if id_r_prov is None or id_th is None:
            return False
        if not isinstance(id_th, int):
            if isinstance(id_th, str):
                try:
                    dict_key_th = int(id_th)
                except ValueError as err:
                    dict_key_th = hash(id_th)
            else:
                dict_key_th = hash(id_th)
        else:
            dict_key_th = id_th
        if not isinstance(id_r_prov, int):
            if isinstance(id_r_prov, str):
                try:
                    dict_key_r_prov = int(id_r_prov)
                except ValueError as err:
                    dict_key_r_prov = hash(id_r_prov)
            else:
                dict_key_r_prov = hash(id_r_prov)
        else:
            dict_key_r_prov = id_r_prov
        if id_db is not None:
            if not isinstance(id_db, int):
                dict_key_db = hash(id_db)
            else:
                dict_key_db = id_db
        else:
            dict_key_db = None
        self.id_maps[0][dict_key_r_prov] = (dict_key_th, dict_key_db)
        self.id_maps[1][dict_key_th] = (dict_key_r_prov, dict_key_db)
        if dict_key_db is not None:
            self.id_maps[2][dict_key_db] = (dict_key_r_prov, dict_key_th)
        return True


    def create_vehicle(self, vehicle_id: int, src: int) -> Union[GenericGTFSItemClass, None]:
        """
        Create a vehicle object and add it to the corresponding dictionary based on the given parameters.

        Parameters:
        - vehicle_id: An integer representing the vehicle ID.
        - src: An integer representing the source of the vehicle data.

        Returns:
        - An instance of the GenericGTFSItemClass representing the created vehicle object, or None.

        """
        return_obj = None
        with self.obj_lock:

            if not isinstance(vehicle_id, int):
                if isinstance(vehicle_id, str):
                    try:
                        dict_key = int(vehicle_id)
                    except ValueError as err:
                        dict_key = hash(vehicle_id)
                else:
                    dict_key = hash(vehicle_id)
            else:
                dict_key = vehicle_id

            create_obj = False
            if src in [UpdateLinkSources.PSG_API.value , UpdateLinkSources.TH_API.value]:
                # th SRC
                found_item = False
                key_r_prov = None
                if self.correlation_tuple is not None:

                    lp_der = None
                    # check for pre-existence in the th dictionary
                    if dict_key in self.vehicle_dict_th:
                        return_obj = self.vehicle_dict_th[dict_key]
                    else:
                        # Search th_ID in TH->LP List and GET LICENSE_PLATE
                        if dict_key in self.correlation_tuple[1]:
                            lp_der = self.correlation_tuple[1][dict_key]
                        if lp_der is not None:
                            # found entry,find the LP in r_prov dictionary
                            for key, val in self.vehicle_dict_r_prov.items():
                                tmp: GenericGTFSItemClass = val
                                lp_r_prov = tmp.get_license_plate()
                                # Match LP in r_prov data to correlation
                                if lp_r_prov is not None:
                                    if compare_license_strings(lp_der, lp_r_prov):
                                        # save r_prov KEY
                                        key_r_prov = key
                                        found_item = True
                                        break
                        if found_item:
                            # assimilate if found and push to th dictionary
                            return_obj = self.vehicle_dict_r_prov[key_r_prov]
                            self.vehicle_dict_th[dict_key] = (self.vehicle_dict_r_prov[key_r_prov])
                        else:
                            create_obj = False
            else:
                # r_prov SRC
                if dict_key in self.vehicle_dict_r_prov:
                    return_obj = self.vehicle_dict_r_prov[dict_key]
                else:
                    create_obj = True

            if create_obj:
                vehicle_obj = GenericGTFSItemClass(vehicle_id, pos_buffer_size=self.pos_buffer_size,
                                                   prop_buffer_size=self.prop_buffer_size)
                if src == UpdateLinkSources.TH_API.value or src == UpdateLinkSources.PSG_API.value:
                    self.vehicle_dict_th[dict_key]: GenericGTFSItemClass = vehicle_obj
                else:
                    self.vehicle_dict_r_prov[dict_key]:GenericGTFSItemClass = vehicle_obj

                return_obj = vehicle_obj

        return return_obj


    def get_vehicle(self, vehicle_id: int, src: int) -> Union[GenericGTFSItemClass, None]:
        """

        This method retrieves a vehicle from the vehicle dictionary based on the provided vehicle ID and source.

        Parameters:
        - vehicle_id (int): The ID of the vehicle to retrieve.
        - src (int): The source of the vehicle information. Should be one of the UpdateLinkSources.TH_API or UpdateLinkSources.PSG_API values.

        Returns:
        - Union[GenericGTFSItemClass, None]: The retrieved vehicle object from the dictionary. Returns None if the vehicle is not found.

        """
        return_val = None
        with self.obj_lock:
            if not isinstance(vehicle_id, int):
                if isinstance(vehicle_id, str):
                    try:
                        dict_key = int(vehicle_id)
                    except ValueError as err:
                        dict_key = hash(vehicle_id)
                else:
                    dict_key = hash(vehicle_id)
            else:
                dict_key = vehicle_id
            if src == UpdateLinkSources.TH_API.value or src == UpdateLinkSources.PSG_API.value:
                if dict_key in self.vehicle_dict_th:

                    return_val = self.vehicle_dict_th[dict_key]
                else:
                    # TH->LP
                    if self.correlation_tuple is not None:
                        if dict_key in self.correlation_tuple[1]:
                            lp_key = self.correlation_tuple[1][dict_key]
                            for key_rad, val in self.vehicle_dict_r_prov.items():
                                val: GenericGTFSItemClass = val
                                lp = val.get_license_plate()
                                if compare_license_strings(lp, lp_key):
                                    return_val = val
                                    self.vehicle_dict_th[dict_key] = val
                                    break
            else:
                if dict_key in self.vehicle_dict_r_prov:
                    return_val = self.vehicle_dict_r_prov[dict_key]

        return return_val

    def refresh_serial_cache(self) -> list[dict]:
        """

        Refreshes the serial cache and returns the updated cache as a list of dictionaries.

        Returns:
            A list of dictionaries representing the updated serial cache.

        """
        return self.gen_output_serial()

    def get_serial_output_cache(self) -> list[dict]:
        """
        Retrieve the serial output cache.

        :return: A copy of the serial output cache as a list of dictionaries.
        :rtype: list[dict]
        """
        k = None
        with self.lock_http_thread:
            x = self.serial_out_cache
            if x is not None:
                k = copy.deepcopy(x)
            else:
                k = copy.deepcopy(self.refresh_serial_cache())
        return k

    def gen_output_serial(self) -> list[dict]:
        """
        This method `gen_output_serial` generates output in serial format by processing the data stored in the `vehicle_dict_r_prov` dictionary.

        The method returns a list of dictionaries, where each dictionary represents a vehicle and its associated information.

        The function follows these steps:
        1. Acquire the lock (`obj_lock`) to ensure thread safety.
        2. Initialize an empty list named `export_data` to store the generated output.
        3. Get the current timestamp in UTC.
        4. Iterate through each vehicle in the `vehicle_dict_r_prov` dictionary.
            - Get the vehicle's record timestamp, message ID, and source.
            - If the timestamp is not None, convert it to ISO format.
            - Create a dictionary (`dict_obj`) with the vehicle's ID, timestamp, and source.
            - Get the current trip ID.
            - Create a dictionary (`trip`) with the trip ID.
            - Get the route ID and assign it to the `route_id_tmp` variable.
            - If `route_id_tmp` is not None and greater than 0, assign it to `route_id`; otherwise, assign 0 or None.
            - Add the `route_id` and direction ID (based on the vehicle's direction) to the `trip` dictionary.
            - Get the trip start time.
            - If the start time is not None, convert it to ISO format; otherwise, assign None.
            - Create a dictionary (`vehicle_sub_dict`) to store additional vehicle information.
            - If the vehicle ID is a tuple, assign the first element as the vehicle ID.
            - If the vehicle ID is not a tuple, try to convert it to an integer; if not possible, hash the vehicle ID.
            - Get the vehicle's license plate.
            - Create a dictionary (`position`) to store the vehicle's position information.
            - Get the latitude, longitude, and timestamp of the vehicle's position.
            - If the position exists, assign the values to the `position` dictionary; otherwise, assign None.
            - Create a dictionary (`passenger_info`) to store passenger information.
            - Get the number of passengers on board, timestamp, in count, and out count.
            - If passenger information exists, assign the values to the `passenger_info` dictionary; otherwise, assign None.
            - Create a dictionary (`speed_obj`) to store speed information.
            - Get the vehicle's velocity tuple.
            - If the velocity tuple exists, assign it to the `speed_obj` dictionary; otherwise, assign None.
            - Add the `trip`, `vehicle_sub_dict`, `position`, and `passenger_info` dictionaries to the `dict_obj` dictionary.
            - Append the `dict_obj` to the `export_data` list.
        5. Acquire the conditional variable (`lock_http_thread`) lock to ensure thread safety.
        6. Copy `export_data` into the `serial_out_cache`.
        7. Update the `output_cache_timestamp` to the current timestamp.
        8. Return the `export_data` list.

        Note: The method assumes that certain classes and variables are defined externally and might not provide detailed information about their functionality.
        """
        export_data = list()
        now_ts = datetime.datetime.now(tz=datetime.timezone.utc)
        with self.sql_obj.lock:
            correlation_dict_lp = copy.deepcopy(self.sql_obj.vehicle_correlation_dict_lp)
        with self.obj_lock:





            for vehicle_id, vehicle in self.vehicle_dict_r_prov.items():
                timestamp_utc = vehicle.get_record_timestamp()
                vehicle: GenericGTFSItemClass = vehicle
                id_msg = vehicle.get_msg_id() if vehicle.get_msg_id() is not None else 0
                if timestamp_utc is not None:
                    timestamp_utc = (timestamp_utc.isoformat())
                dict_obj = dict(id=id_msg, timestamp=timestamp_utc, src=vehicle.get_record_src())
                trip_id = vehicle.get_current_trip_id()
                trip = dict(trip_id=trip_id)
                route_id_tmp = vehicle.get_route_id()
                route_id = (int(route_id_tmp) if route_id_tmp > 0 else 0) if route_id_tmp is not None else None
                del route_id_tmp
                trip['route_id'] = route_id
                trip['direction_id'] = 0 if vehicle.get_direction() else 1
                start_time: Union[None, datetime.datetime] = vehicle.get_trip_start_time()
                if start_time is None:
                    trip['start_time'] = None
                else:
                    trip['start_time'] = start_time.isoformat() if isinstance(start_time,datetime.datetime) else str(start_time)
                vehicle_sub_dict = dict()
                if isinstance(vehicle_id, tuple):

                    vehicle_sub_dict['id'] = int(vehicle_id[0])
                else:
                    try:
                        vehicle_sub_dict['id'] = int(vehicle_id)
                    except ValueError:
                        vehicle_sub_dict['id'] = hash(vehicle_id)
                vehicle_sub_dict['th_id'] = None
                vehicle_sub_dict['license_plate'] = vehicle.get_license_plate()
                lp:Union[None,str] = vehicle_sub_dict['license_plate']
                try:
                    if (lp is not None) and (str(lp) in correlation_dict_lp):
                        vehicle_sub_dict['th_id'] = correlation_dict_lp[str(lp)]
                except (KeyError,ValueError):
                    pass
                position = dict()
                pos = vehicle.get_pos()
                if pos is not None:
                    position['latitude'] = pos[0]
                    position['longitude'] = pos[1]
                    position['timestamp'] = pos[2].isoformat() if pos[2] is not None else None
                else:
                    position['latitude'] = None
                    position['longitude'] = None
                    position['timestamp'] = None
                passenger_info = dict()
                psg_raw = vehicle.get_current_psg_tuple()
                if psg_raw is not None:
                    passenger_info['on_board'] = int(psg_raw[0])
                    passenger_info['timestamp'] = psg_raw[1].isoformat() if psg_raw[1] is not None else None
                    passenger_info['in'] = int(psg_raw[2])
                    passenger_info['out'] = int(psg_raw[3])
                else:
                    passenger_info['on_board'] = None
                    passenger_info['timestamp'] = None
                    passenger_info['in'] = None
                    passenger_info['out'] = None
                speed_obj = dict()
                velocity_tuple = vehicle.get_velocity()
                if velocity_tuple is not None:
                    speed_obj["velocity"] = velocity_tuple
                else:
                    speed_obj["velocity"] = None
                dict_obj['vehicle'] = dict(trip=trip, vehicle=vehicle_sub_dict, position=position,
                                           passenger_info=passenger_info)

                export_data.append(dict_obj)

        with self.lock_http_thread:
            self.serial_out_cache.clear()
            self.serial_out_cache.extend(export_data)
            self.output_cache_timestamp = now_ts

        del now_ts
        del correlation_dict_lp
        return export_data

    def gen_output(self) -> list[dict]:
        """
        Generate output data for vehicles.

        Returns:
            List of dictionaries representing the output data for vehicles.

        Example:
            >>> gen_output()
            [
                {
                    'id': 12345,
                    'timestamp': '2022-01-01 00:00:00',
                    'src': 'source',
                    'vehicle': {
                        'trip': {
                            'trip_id': 'trip123',
                            'route_id': 'route123',
                            'direction_id': 0,
                            'start_time': '2022-01-01 00:00:00'
                        },
                        'id': 12345,
                        'license_plate': 'ABC123',
                        'position': {
                            'latitude': 123.456,
                            'longitude': 789.012,
                            'timestamp': '2022-01-01 00:00:00'
                        },
                        'passenger_info': {
                            'on_board': 5,
                            'timestamp': '2022-01-01 00:00:00',
                            'in': 2,
                            'out': 1
                        }
                    }
                },
                ...
            ]
        """
        export_data = []
        with self.obj_lock:
            for vehicle_id, vehicle in self.vehicle_dict_r_prov.items():
                timestamp_utc = vehicle.get_record_timestamp()
                vehicle: GenericGTFSItemClass = vehicle
                if not vehicle.get_update_output_flag():
                    continue
                id_msg = vehicle.get_msg_id() if vehicle.get_msg_id() is not None else 0
                dict_obj = dict(id=id_msg, timestamp=timestamp_utc, src=vehicle.get_record_src())
                trip_id = vehicle.get_current_trip_id()
                trip = dict(trip_id=trip_id)
                route_id_tmp = vehicle.get_route_id()
                route_id = (int(route_id_tmp) if route_id_tmp > 0 else 0) if route_id_tmp is not None else None
                del route_id_tmp
                trip['route_id'] = route_id
                trip['direction_id'] = 0 if vehicle.get_direction() else 1
                start_time: Union[None, datetime.datetime] = vehicle.get_trip_start_time()
                if start_time is None:
                    trip['start_time'] = None
                else:
                    trip['start_time'] = start_time
                vehicle_sub_dict = dict()
                if isinstance(vehicle_id, tuple):

                    vehicle_sub_dict['id'] = int(vehicle_id[0])
                else:
                    try:
                        vehicle_sub_dict['id'] = int(vehicle_id)
                    except ValueError:
                        vehicle_sub_dict['id'] = hash(vehicle_id)

                vehicle_sub_dict['license_plate'] = vehicle.get_license_plate()
                position = dict()
                pos = vehicle.get_pos()
                if pos is not None:
                    position['latitude'] = pos[0]
                    position['longitude'] = pos[1]
                    position['timestamp'] = pos[2]
                else:
                    position['latitude'] = None
                    position['longitude'] = None
                    position['timestamp'] = None
                passenger_info = dict()
                psg_raw = vehicle.get_current_psg_tuple()
                if psg_raw is not None:
                    passenger_info['on_board'] = psg_raw[0]
                    passenger_info['timestamp'] = psg_raw[1]
                    passenger_info['in'] = psg_raw[2]
                    passenger_info['out'] = psg_raw[3]
                else:
                    passenger_info['on_board'] = None
                    passenger_info['timestamp'] = None
                    passenger_info['in'] = None
                    passenger_info['out'] = None
                speed_obj = dict()
                velocity_tuple = vehicle.get_velocity()
                if velocity_tuple is not None:
                    speed_obj["velocity"] = velocity_tuple
                else:
                    speed_obj["velocity"] = None
                dict_obj['vehicle'] = dict(trip=trip, vehicle=vehicle_sub_dict, position=position,
                                           passenger_info=passenger_info)

                export_data.append(dict_obj)

        return export_data

    def generic_std_update(self, object_in: Union[dict, list], src: int) -> bool:
        """
        Updates the generic standard vehicle information based on the input object and source.

        Parameters:
        - object_in: Union[dict, list]
            - The input object containing the vehicle information.
            - If it is a list, the method will update each vehicle in the list.
            - If it is a dictionary, the method will update the vehicle based on the dictionary.
        - src: int
            - The source of the update.

        Returns:
        - bool
            - True if the update is successful, False otherwise.
        """
        if isinstance(object_in, list):
            k = True
            for i in object_in:
                k = k and self.generic_std_update(i, src)
                if not k:
                    break
            return k
        else:
            if "id" in object_in and 'timestamp' in object_in and 'trip' in object_in and 'position' in object_in:
                try:
                    vehicle_id = int(object_in['vehicle']['vehicle']['id'])
                    targ = self.get_vehicle(vehicle_id, UpdateLinkSources.Generic.value)
                    if targ is None:
                        targ: GenericGTFSItemClass = self.create_vehicle(vehicle_id, UpdateLinkSources.Generic.value)
                    if targ is None:
                        return False
                    msg_id = int(object_in['id'])
                    targ.set_msg_id(msg_id)
                    timestamp_msg = object_in['timestamp']
                    if isinstance(timestamp_msg, str):
                        timestamp_msg = datetime.datetime.fromisoformat(timestamp_msg)
                    elif not isinstance(timestamp_msg, datetime.datetime):
                        timestamp_msg = datetime.datetime.now(tz=datetime.timezone.utc)
                    targ.set_record_timestamp(timestamp_msg, src)
                    position_obj = object_in['position']

                    if 'latitude' in position_obj and 'longitude' in position_obj:

                        lat = float(position_obj['latitude'])
                        long = float(position_obj['longitude'])
                        ts = None
                        if 'timestamp' in position_obj:
                            ts = position_obj['timestamp']
                            if isinstance(ts, str):
                                ts = datetime.datetime.fromisoformat(ts)
                        else:
                            ts = timestamp_msg
                        targ.push_pos(lat, long, ts)
                        size = targ.get_pos_fifo_size()
                        if size >= 2:
                            p1: tuple[float, float, datetime.datetime] = targ.get_pos_by_index(-1)
                            p2: tuple[float, float, datetime.datetime] = targ.get_pos_by_index(-2)
                            if abs(p1[2] - p2[2]).total_seconds() < 10:
                                speed_t = abs(p1[2] - p2[2]).total_seconds()
                                speed_d = geodesic((p1[0], p1[1]), (p2[0], p2[1])).m
                                speed = speed_d / speed_t
                                time_stamp_speed = p1[2] if p1[2] > p2[2] else p2[2]
                                if speed <= 100:
                                    targ.push_velocity(float(speed), time_stamp_speed)

                    license_plate = str(object_in['vehicle']['vehicle']['license_plate'])
                    if targ.get_license_plate() is None or targ.get_license_plate() != license_plate:
                        targ.set_license_plate(license_plate)

                    if 'route_id' in object_in['trip']:
                        targ.set_route_id(int(object_in['trip']['route_id']))
                    else:
                        targ.set_route_id(-1)

                    if 'trip_id' in object_in['trip']:
                        targ.set_current_trip_id(object_in['trip']['trip_id'])
                    else:
                        targ.set_current_trip_id("None")
                    if 'start_time' in object_in['trip']:
                        start_time = object_in['trip']['start_time']
                        ok = True
                        if isinstance(start_time, str):
                            try:
                                start_time = datetime.datetime.strptime(start_time, "%H:%M:%S").time()
                                start_time = datetime.datetime.combine(timestamp_msg.date(), start_time,
                                                                       tzinfo=datetime.timezone.utc)
                            except ValueError as error:
                                print(f"DateTime processing error: {error}")
                                ok = False
                                start_time = timestamp_msg
                        if ok:
                            targ.set_trip_start_time(start_time)
                    if 'passenger_info' in object_in:
                        passenger_info = object_in['passenger_info']
                        timestamp_psg = None
                        if 'timestamp' in passenger_info:
                            if isinstance(passenger_info['timestamp'], str):
                                timestamp_psg = datetime.datetime.fromisoformat(passenger_info['timestamp'])
                            elif isinstance(passenger_info['timestamp'], datetime.datetime):
                                timestamp_psg = passenger_info['timestamp']
                            else:
                                timestamp_psg = timestamp_msg
                        targ.set_current_psg_number(int(passenger_info['on_board']), timestamp_psg)
                        targ.set_update_output_flag()
                except (KeyError, ValueError, TypeError) as error:
                    print(f"Error occurred while generating/processing vehicle: {error}")
                    return False
            else:
                return False

    def update_from_s_api(self, obj_in: Prov_R_LinkInMemObj) -> bool:
        """
        Updates the internal state of the object based on data received from an S Provider API.

        Parameters:
        - obj_in: An instance of the Prov_R_LinkInMemObj class, representing the data received from the S Provider API.

        Returns:
        - bool: True if the update was successful, False otherwise.

        Raises:
        - KeyError: If a vehicle or other required data is missing from the received data.
        - ValueError: If the received data has invalid values.

        """
        stat = 0
        if isinstance(obj_in, Prov_R_LinkInMemObj):
            update_obj = Prov_R_LinkInMemObj.process_data(obj_in.get_in_mem_last())
            if update_obj is not None:
                if isinstance(update_obj, list):
                    for j in update_obj:
                        for i in j:
                            msg_id = i["id"]
                            vehicle_id = i.vehicle.vehicle.id
                            try:
                                ts = datetime.datetime.fromisoformat(i["timestamp_utc"])
                                gtfs_vehicle_obj: GenericGTFSItemClass = self.vehicle_dict_r_prov[str(vehicle_id)]
                                gtfs_vehicle_obj.set_record_timestamp(ts, UpdateLinkSources.R_API.value)
                                gtfs_vehicle_obj.set_msg_id(int(msg_id))
                                gtfs_vehicle_obj.set_license_plate(str(i.vehicle.vehicle.license_plate))
                                gtfs_vehicle_obj.push_pos(float(i.vehicle.postion.latitude),
                                                          float(i.vehicle.postion.longitude),
                                                          ts)
                                if gtfs_vehicle_obj.get_pos_fifo_size() > 2:
                                    u = gtfs_vehicle_obj.get_pos_by_index(-2)
                                    y = (u[0], u[1])

                                    delta_d = geodesic((i.vehicle.postion.latitude, i.vehicle.postion.longitude),
                                                       y).m
                                    delta_t: datetime.timedelta = abs(u[2] - ts)
                                    delta_t_s = delta_t.total_seconds()
                                    v = float(delta_d / delta_t_s)
                                    gtfs_vehicle_obj.push_velocity(v, ts)

                                if i.trip.route_id is not None:
                                    gtfs_vehicle_obj.set_route_id(int(i.trip.route_id))
                                else:
                                    gtfs_vehicle_obj.clear_route_id()

                                if i.trip.trip_id is not None:
                                    gtfs_vehicle_obj.set_current_trip_id(str(i.trip.trip_id))
                                else:
                                    gtfs_vehicle_obj.set_current_trip_id("None")

                                if i.trip.direction_id is not None:
                                    gtfs_vehicle_obj.set_direction(i.trip.direction_id)
                                else:
                                    gtfs_vehicle_obj.set_direction(False)
                                if i.trip.start_time is not None:
                                    gtfs_vehicle_obj.set_trip_start_time(i.trip.start_time)
                                gtfs_vehicle_obj.set_update_output_flag()
                            except KeyError:
                                print(f"Key Error for vehicle {vehicle_id}")
                                stat = stat + 1
                            except ValueError:
                                print(f"Value Error for vehicle {vehicle_id}")
                                stat = stat + 1

                else:
                    for i in update_obj:
                        msg_id = i.id
                        vehicle_id = i.vehicle.id
                        try:
                            ts = datetime.datetime.fromisoformat(i["timestamp_utc"])
                            gtfs_vehicle_obj = self.vehicle_dict_r_prov[str(vehicle_id)]
                            gtfs_vehicle_obj.set_record_timestamp(ts,
                                                                  UpdateLinkSources.R_API.value)
                            gtfs_vehicle_obj.set_msg_id(int(msg_id))
                            gtfs_vehicle_obj.set_license_plate(str(i.vehicle.vehicle.license_plate))
                            gtfs_vehicle_obj.push_pos(float(i.vehicle.postion.latitude),
                                                      float(i.vehicle.postion.longitude),
                                                      ts)
                            if i.trip.route_id is not None:
                                gtfs_vehicle_obj.set_route_id(int(i.trip.route_id))
                            else:
                                gtfs_vehicle_obj.set_route_id(-1)

                            if i.trip.trip_id is not None:
                                gtfs_vehicle_obj.set_current_trip_id(str(i.trip.trip_id))
                            else:
                                gtfs_vehicle_obj.set_current_trip_id("None")

                            if i.trip.direction_id is not None:
                                gtfs_vehicle_obj.set_direction(i.trip.direction_id)
                            else:
                                gtfs_vehicle_obj.set_direction(False)
                            if i.trip.start_time is not None:
                                gtfs_vehicle_obj.set_trip_start_time(i.trip.start_time)
                            gtfs_vehicle_obj.set_update_output_flag()
                        except KeyError:
                            print(f"Key Error for vehicle {vehicle_id}")
                            stat = stat + 1
                        except ValueError:
                            print(f"Value Error for vehicle {vehicle_id}")
                            stat = stat + 1
        return stat

    def update_from_int_api(self, obj_in) -> bool:
        """
        Updates the object from Internal Server API data.

        Parameters:
        - obj_in: The object to be updated with Internal Server API data.

        Returns:
        - bool: Returns True if the object was successfully updated, False otherwise.
        """
        if obj_in is None:
            return False
        if isinstance(obj_in, list):
            k = True
            for i in obj_in:
                k = k and self.update_from_int_api(i)
            return k
        else:
            try:
                k = True
                msg_id = int(obj_in['id'])
                timestamp_msg = obj_in['timestamp']
                raw_id = None
                if 'vehicle' in obj_in['vehicle']:
                    if 'id' in obj_in['vehicle']['vehicle']:
                        raw_id = obj_in['vehicle']['vehicle']['id']
                if raw_id is None or msg_id is None or timestamp_msg is None:
                    return False
                try:
                    vehicle_id = int(raw_id)
                except ValueError:
                    vehicle_id = hash(raw_id)
                targ: GenericGTFSItemClass = self.get_vehicle(vehicle_id, UpdateLinkSources.Internal_SRV.value)
                if targ is None:
                    targ: GenericGTFSItemClass = self.create_vehicle(vehicle_id, UpdateLinkSources.Internal_SRV.value)
                if targ is None:
                    return False
                with self.obj_lock:
                    if 'vehicle' not in obj_in:
                        k = False
                    else:
                        if 'trip' in obj_in['vehicle']:
                            route_id = obj_in['vehicle']['trip']['route_id']
                            trip_id = obj_in['vehicle']['trip']['trip_id']
                            direction_id = obj_in['vehicle']['trip']['direction_id']
                            start_time = obj_in['vehicle']['trip']['start_time']
                            if route_id is not None:
                                targ.set_route_id(route_id)
                            else:
                                targ.clear_route_id()
                            if trip_id is not None:
                                targ.set_current_trip_id(trip_id)
                            else:
                                targ.set_current_trip_id(None)
                            if direction_id is not None:
                                tmp_dir: bool = False if direction_id > 0 else True
                                targ.set_direction(tmp_dir)
                            else:
                                targ.set_direction(None)
                            if start_time is not None:
                                targ.set_trip_start_time(start_time)
                            else:
                                targ.set_trip_start_time(None)
                        else:
                            k = False
                    if 'vehicle' in obj_in['vehicle']:
                        lic: str = str(obj_in['vehicle']['vehicle']['license_plate'])
                        if lic is not None:
                            if not compare_license_strings(
                                    targ.get_license_plate() if targ.get_license_plate() is not None else "", lic):
                                targ.set_license_plate(lic.replace(" ", "-").replace(".", "-"))
                    else:
                        k = False
                    if 'position' in obj_in['vehicle']:
                        pos = obj_in['vehicle']['position']
                        lat = pos['latitude']
                        long = pos['longitude']
                        ts = pos['timestamp']
                        if pos is None or lat is None or long is None:
                            k = False
                        else:
                            targ.push_pos(lat, long, ts)
                    else:
                        k = False
                    targ.set_record_timestamp(timestamp_msg, UpdateLinkSources.Internal_SRV.value)
                    targ.set_msg_id(msg_id)
                    targ.set_update_output_flag()
                return k
            except (KeyError,ValueError):
                return False

    def update_from_th(self, obj_in) -> bool:
        """

        Updates the object from Th data.

        :param obj_in: The input object to update from Th data.
        :type obj_in: Union[list, dict]

        :return: True if the update is successful, False otherwise.
        :rtype: bool

        """
        if isinstance(obj_in, list):
            k = True
            for i in obj_in:
                k = k and self.update_from_th(i)
            return k
        elif isinstance(obj_in, dict):
            timestamp_mem = None
            
            for key_nr, value_obj in obj_in.items():
                if isinstance(value_obj, datetime.datetime):
                    timestamp_mem = value_obj
                    continue
                vehicle_id = None
                try:
                    vehicle_id = int(key_nr)
                except (ValueError, KeyError) as err:
                    print(f"Key Error for vehicle key {key_nr}\n Value:{value_obj} \n Object IN:{obj_in}\n")
                    return False
                targ: GenericGTFSItemClass = self.get_vehicle(vehicle_id, UpdateLinkSources.TH_API.value)
                if targ is None:
                    targ = self.create_vehicle(vehicle_id, UpdateLinkSources.TH_API.value)
                if targ is None:
                    return False
                if 'timestamp_reception' in value_obj:
                    if value_obj['timestamp_reception'] is not None:
                        timestamp_msg = value_obj['timestamp_reception']
                    else:
                        timestamp_msg = timestamp_mem
                else:
                    timestamp_msg = timestamp_mem
                with self.obj_lock:
                    targ.set_record_timestamp(timestamp_msg, UpdateLinkSources.TH_API.value)

                    pos: Union[None, tuple[float, float, datetime.datetime]] = None
                    if 'lat' in value_obj and 'lon' in value_obj:
                        if value_obj['lat'] is not None and value_obj['lon'] is not None:
                            pos = (float(value_obj['lat']), float(value_obj['lon']), timestamp_msg)

                    if pos is not None:
                        targ.push_pos(pos[0], pos[1], pos[2])

                    velocity = None
                    if 'speed' in value_obj:
                        if value_obj['speed'] is not None:
                            velocity = float(value_obj['speed'])
                    if 'speed_aux' in value_obj:
                        if value_obj['speed_aux'] is not None:
                            if velocity is not None:
                                velocity += float(value_obj['speed_aux'])
                                velocity = velocity / 2.0
                            else:
                                velocity = float(value_obj['speed_aux'])
                    if velocity is not None:
                        targ.push_velocity(velocity, timestamp_msg)

                    msg_id = hash((timestamp_msg, pos))
                    targ.set_msg_id(msg_id)

                    psg_count = None
                    if 'psg_count_total' in value_obj:
                        if value_obj['psg_count_total'] is not None:
                            psg_count = int(value_obj['psg_count_total'])
                    if psg_count is not None:
                        targ.set_current_psg_number(psg_count, timestamp_msg)

                    route_id = None
                    if 'line_id' in value_obj:
                        if value_obj['line_id'] is not None:
                            try:
                                route_id = int(value_obj['line_id'])
                            except ValueError:
                                route_id = None
                    if route_id is not None:
                        targ.set_route_id(route_id)
                    else:
                        targ.clear_route_id()
                    targ.set_update_output_flag()

        else:
            return False

    def update_from_psg_api(self, obj_in) -> bool:
        """
        Update the database with data from the th PSG API.

        :param obj_in: The input object containing the data to update.
        :type obj_in: Union[dict, list]
        :return: True if the update is successful, False otherwise.
        :rtype: bool

        This method updates the database with data obtained from the th PSG API. The input object
        can be either a dictionary or a list. If it's a list, each element is passed to the
        "update_from_th" method and the results are AND-ed together. If it's a dictionary,
        each key-value pair is processed.

        If the key is of type int, it is interpreted as a vehicle ID for which the data should be updated.
        A target vehicle is obtained using the "get_vehicle" method with the vehicle ID and the
        UpdateLinkSources.PSG_API value. If the target vehicle doesn't exist, a new vehicle is created
        using the "create_vehicle" method with the vehicle ID and UpdateLinkSources.PSG_API value.
        If either of these operations fails, the processing continues with the next key-value pair.

        The value of the dictionary should have the following structure:

        {
            "timestamp": <timestamp>,
            "capacity": <capacity>,
            "on_board": <on_board>,
            "apc_sensors": {
                "in": <apc_sensor_in>,
                "out": <apc_sensor_out>
            }
        }

        where:
        - timestamp (str, required): The timestamp associated with the data.
        - capacity (int, required): The capacity of the vehicle.
        - on_board (int, required): The number of passengers on board.
        - apc_sensor_in (int, required): The number of passengers entering the vehicle.
        - apc_sensor_out (int, required): The number of passengers exiting the vehicle.

        The target vehicle is updated with the on_board, timestamp, apc_sensor_in, apc_sensor_out,
        capacity, and record_timestamp values obtained from the input dictionary. The update flag
        is also set to indicate that the vehicle has been updated.

        Example usage:
        # Update a single vehicle
        vehicle_data = {
            "timestamp": "2022-01-01 10:00:00",
            "capacity": 50,
            "on_board": 30,
            "apc_sensors": {
                "in": 10,
                "out": 5
            }
        }
        update_from_th_psg_api(vehicle_data)

        # Update multiple vehicles
        vehicle_list = [
            vehicle_data_1,
            vehicle_data_2,
            vehicle_data_3
        ]
        update_from_th_psg_api(vehicle_list)
        """
        if isinstance(obj_in, list):
            k = True
            for i in obj_in:
                k = k and self.update_from_th(i)
            return k
        elif isinstance(obj_in, dict):
            for key, value_obj in obj_in.items():
                if isinstance(value_obj, dict):

                    try:
                        vehicle_id = int(key)
                    except (ValueError, KeyError) as err:
                        #print(f"Key Error for vehicle key {key}\n Value:{value_obj} \n Object IN:{obj_in}\n")
                        continue

                    targ: GenericGTFSItemClass = self.get_vehicle(vehicle_id, UpdateLinkSources.PSG_API.value)
                    if targ is None:
                        targ = self.create_vehicle(vehicle_id, UpdateLinkSources.PSG_API.value)
                    if targ is None:
                        continue
                    timestamp_psg = value_obj['timestamp']
                    now = datetime.datetime.now(tz=datetime.timezone.utc)
                    capacity = int(value_obj['capacity'])
                    on_board = int(value_obj['on_board'])
                    apc_sensor_in = value_obj['apc_sensors']['in']
                    apc_sensor_out = value_obj['apc_sensors']['out']
                    with self.obj_lock:
                        if on_board is not None and timestamp_psg is not None:
                            targ.set_current_psg_number_extended(on_board, timestamp_psg, apc_sensor_in, apc_sensor_out)

                            targ.set_vehicle_capacity(capacity)

                            targ.set_record_timestamp(now, UpdateLinkSources.PSG_API.value)
                            targ.set_update_output_flag()
            return True


class PsgQuery:
    """
    A class that represents a th PSG query.

    Attributes:
        username (str): The username used for authentication.
        password (str): The password used for authentication.
        url (str): The URL of the th PSG endpoint.
        session (requests.Session): A session object used to make HTTP requests.

    Methods:
        parse_psg_data(data_in: list) -> Union[dict, None]:
            Parse the given PSG data and return a dictionary with vehicle_id keys.

        fetch_update() -> Union[dict, list, None]:
            Fetch update from the th PSG endpoint and return the parsed data or None if there was an error.
    """
    def __init__(self, username: str, password: str):
        self.passwd = password
        self.username = username
        self.url = "https://station.tpbi.ro/th_psg"
        self.session = requests.Session()
        self.session.auth = (self.username, self.passwd)
        self.obj_lock = threading.RLock()

    #   parse data and return dict with vehicle_id keys
    @staticmethod
    def parse_psg_data(data_in: list) -> Union[dict, None]:
        """

        Parse PSD Data

        This method is used to parse the data received from the PSD API.

        Parameters:
        - data_in (list): The input data to be parsed.

        Returns:
        - Union[dict, None]: Returns a dictionary containing the parsed data, or None if the input data is invalid.

        Example Usage:
        ```python
        data = [
            {
                'vehicle_id': '1234',
                'data_timestamp': 1615270612000,
                'people_on_board': '5',
                'capacity': '10',
                'apc_sensors': [
                    {
                        'entries': '2',
                        'exits': '1'
                    },
                    {
                        'entries': '3',
                        'exits': '2'
                    }
                ]
            },
            {
                'vehicle_id': '5678',
                'data_timestamp': 1615270613000,
                'people_on_board': '8',
                'capacity': '12',
                'apc_sensors': [
                    {
                        'entries': '4',
                        'exits': '3'
                    },
                    {
                        'entries': '2',
                        'exits': '1'
                    }
                ]
            }
        ]

        parsed_data = parse_psg_data(data)
        print(parsed_data)
        ```
        ```python
        {
            1234: {
                'vehicle_id': 1234,
                'timestamp': datetime.datetime(2021, 3, 9, 10, 16, 52, tzinfo=datetime.timezone.utc),
                'on_board': 5,
                'capacity': 10,
                'apc_sensors': {
                    'in': 5,
                    'out': 3
                }
            },
            5678: {
                'vehicle_id': 5678,
                'timestamp': datetime.datetime(2021, 3, 9, 10, 16, 53, tzinfo=datetime.timezone.utc),
                'on_board': 8,
                'capacity': 12,
                'apc_sensors': {
                    'in': 6,
                    'out': 4
                }
            }
        }
        ```

        Note: This method assumes the existence of a global logger object for logging purposes.

        """
        global logger
        if data_in is not None:
            if isinstance(data_in, list):
                now = datetime.datetime.now(tz=datetime.timezone.utc)
                return_data = dict()

                for i_obj in data_in:
                    if not isinstance(i_obj,dict):
                        continue

                    if 'vehicle_id' in i_obj:
                        try:
                            key = int(i_obj['vehicle_id'])
                        except ValueError as err:
                            key = hash(i_obj['vehicle_id'])
                            if logger is not None:
                                logger.debug("PSG API Value Error vehicle_id", exc_info=err)
                    else:
                        continue
                    return_data[key] = {
                                        'vehicle_id': key,
                                        'timestamp': now,
                                        'on_board': 0,
                                        'capacity': 0,
                                        'apc_sensors': {'in': None, 'out': None}
                                        }

                    # Parse timestamp
                    data_timestamp = i_obj.get('data_timestamp')
                    if data_timestamp is not None:
                        try:
                            parsed_timestamp = datetime.datetime.fromtimestamp(data_timestamp / 1000,
                                                                               tz=datetime.timezone.utc)
                            return_data[key]['timestamp'] = parsed_timestamp
                        except ValueError as err:
                            if logger:
                                logger.debug("PSG API Value Error timestamp", exc_info=err,
                                             extra={'data': data_timestamp})

                    # Parse people_on_board
                    people_on_board = i_obj.get('people_on_board')
                    if people_on_board is not None:
                        try:
                            return_data[key]['on_board'] = int(people_on_board)
                        except ValueError:
                            return_data[key]['on_board'] = 0
                    else:
                        return_data[key]['on_board'] = None


                    if 'capacity' in i_obj:
                        return_data[key]['capacity'] = int(i_obj['capacity'])
                    else:
                        return_data[key]['capacity'] = None
                    return_data[key]['apc_sensors'] = dict()
                    if 'apc_sensors' in i_obj:
                        apc_array = i_obj['apc_sensors']
                        in_nr = 0
                        out_nr = 0
                        if isinstance(apc_array, list):
                            for j_obj in apc_array:
                                in_nr += int(j_obj['entries'])
                                out_nr += int(j_obj['exits'])

                        return_data[key]['apc_sensors']['in'] = in_nr
                        return_data[key]['apc_sensors']['out'] = out_nr
                    else:
                        return_data[key]['apc_sensors']['in'] = None
                        return_data[key]['apc_sensors']['out'] = None
                return return_data

            else:
                return None
        else:
            return None

    def fetch_update(self) -> Union[dict, list, None]:
        """

        Fetches and updates data from the specified endpoint.

        Parameters:
        - None

        Returns:
        - Union[dict, list, None]: Returns a dictionary or list if the request to the endpoint is successful and the response can be parsed. Returns None if there is an error during the request or parsing.

        """
        global logger
        try:
            with self.obj_lock:
                with self.session as session:

                    req = session.request("GET", self.url, auth=(self.username, self.passwd), stream=False, timeout=5,
                                          verify=False)

                    if req.status_code == requests.codes.ok:
                        data = req.content
                        try:
                            json_decoded = json.loads(data)
                        except JSONDecodeError as err:

                            if logger is not None:
                                logger.info("JSON Decode Error", exc_info=err, extra={'raw_data': data})
                        req.close()

                        return self.parse_psg_data(json_decoded)
                    else:

                        if logger is not None:
                            logger.info("th HTTP Status Code Error", extra={'status_code': req.status_code})
                        req.close()
                        return None




        except requests.exceptions.RequestException as err:
            if logger is not None:
                logger.info("Request Exception", exc_info=err)
            return None
        except ConnectionError as err:
            if logger is not None:
                logger.info("Connection Exception", exc_info=err)
            return None


class prov_t_api_query:
    """

    prov_t_api_query

    A class representing a query to the th API.

    Attributes:
    - cookies (RequestsCookieJar): The cookies used for authentication.
    - url_update (str): The URL used for updating data.
    - url_get_cookie (str): The URL used for getting the cookie.
    - url_login (str): The URL used for login.
    - headers_login (dict): The headers used for login request.
    - headers_get_cookie (dict): The headers used for getting the cookie request.
    - headers_update (dict): The headers used for update request.
    - session (requests.Session): The session used for making requests.
    - kontor (int): The kontor value.
    - username (str): The username for authentication.
    - password (str): The password for authentication.
    - auth (dict): The authentication data.
    - login_stat (bool): Flag indicating if the login was successful.
    - login_timestamp (datetime.datetime): The timestamp of the login.
    - vehicle_id_list (list): The list of vehicle IDs.

    Methods:
    - do_login(): Performs the login request to authenticate the user.
    - fetch_update_detailed() -> Union[dict, None]: Fetches detailed update data for all vehicles.
    - fetch_update_aux(vehicle: int) -> Union[dict, list, None]: Fetches auxiliary update data for a specific vehicle.
    - fetch_update(): Fetches update data for all vehicles.

    """
    def __init__(self, username: str, password: str):
        self.cookies = None
        self.url_update = "<>"
        self.url_get_cookie = "<>"
        self.url_login = "<>"

        self.headers_login = {
            'Accept-Encoding': 'gzip,deflate',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Cache-Control': 'no-cache',
            'Connection': 'Keep-Alive',
            'Accept': '*/*',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0'
        }
        self.headers_get_cookie = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0',
            'Accept-Encoding': 'gzip,deflate',
            'Connection': 'keep-alive',

        }
        self.headers_update = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0',
            'Accept-Encoding': 'gzip,deflate',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',

        }
        self.session = requests.Session()
        self.kontor = 0
        self.username = username
        self.password = password
        self.auth = {
            'login_user': self.username,
            'login_password': self.password
        }
        self.login_stat = False
        self.login_timestamp = None
        self.vehicle_id_list = list()
        self.obj_lock = threading.RLock()
    def do_login(self):
        global logger
        with self.obj_lock:
            with self.session as session:
                now = datetime.datetime.now(tz=datetime.timezone.utc)
                try:
                    session.cookies.clear()
                    req = session.request(method='GET', url=self.url_get_cookie, headers=self.headers_get_cookie,
                                          timeout=10)
                    if req.status_code == requests.codes.ok:
                        self.cookies = req.cookies

                        req_login = session.request(method="POST", url=self.url_login, headers=self.headers_login,
                                                    data=self.auth, timeout=10)
                        if req_login.status_code == requests.codes.ok:
                            try:
                                if json.loads(req_login.text):
                                    self.login_stat = True
                                    self.login_timestamp = now
                                    return True
                                else:
                                    self.login_stat = False
                                    self.login_timestamp = None
                                    self.cookies = None
                                    return False
                            except json.decoder.JSONDecodeError as err:
                                self.login_stat = False
                                self.login_timestamp = None
                                self.cookies = None
                                if logger is not None:
                                    logger.info("JSON Decode Error", exc_info=err, extra={'raw_data': req_login.text})
                                return False
                        else:
                            if logger is not None:
                                logger.info("th Alert HTTP Status Code Error",
                                            extra={'status_code': req_login.status_code})
                            print(f"th Alert HTTP Status Code Error:{req_login.status_code}")
                    else:
                        if logger is not None:
                            logger.info("th Alert HTTP Status Code Error", extra={'status_code': req.status_code})
                        print(f"th HTTP Status Code Error:{req.status_code}")
                        self.login_stat = False
                        self.login_timestamp = None
                        self.cookies = None
                        return False
                except (
                        requests.exceptions.ConnectionError, requests.exceptions.RetryError,
                        requests.exceptions.ConnectTimeout,
                        requests.exceptions.Timeout) as e:

                    if logger is not None:

                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug("Non-Critical  Connection/Retry/Timeout Exception", exc_info=e)
                        else:
                            logger.info("Non-Critical  Connection/Retry/Timeout Exception")
                    self.login_stat = False
                    self.login_timestamp = None
                    self.cookies = None
                    return False

                except requests.exceptions.RequestException as e:
                    print(f"Request Error:{e}")
                    if logger is not None:
                        logger.info("Request Exception", exc_info=e)
                    self.login_stat = False
                    self.login_timestamp = None
                    self.cookies = None
                    return False

    def fetch_update_detailed(self) -> Union[dict, None]:
        global logger
        with self.obj_lock:
            if len(self.vehicle_id_list) == 0 or not self.login_stat:
                result = self.fetch_update_auto()
                if result is not None:
                    return self.fetch_update_detailed()

                else:
                    return None
            else:
                result_dict = dict()
                now = datetime.datetime.now(tz=datetime.timezone.utc)
                k = 0
                for i in self.vehicle_id_list:
                    try:
                        id_int = int(i)
                        x = random.randint(100, 3000)
                        x = float(x) / 1000
                        time.sleep(x)
                        result = self.fetch_update_aux(id_int)
                        parsed_data = export_mem_object_th(result, now, id_int)
                        for key, val in parsed_data.items():
                            if str(key).isnumeric():
                                int_tmp = int(key)
                                result_dict[int_tmp] = val
                                k = k + 1
                    except requests.exceptions.RequestException as e:
                        self.login_timestamp = None
                        self.cookies = None
                        self.login_stat = False
                        if logger is not None:
                            logging.info("Request Exception", exc_info=e)
                        print(f"Request Error:{e}")
                        return None
                result_dict['timestamp'] = now
                if k > 0:
                    return result_dict
                else:
                    return None

    def fetch_update_aux(self, vehicle: int) -> Union[dict, list, None]:
        global logger
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        with self.obj_lock:
            if self.login_stat and self.login_timestamp is not None:
                if (self.login_timestamp - now) < datetime.timedelta(minutes=40):

                    with self.session as session:
                        req = session.request(method='POST', url=self.url_update, headers=self.headers_update,
                                              data={'vehicle': str(vehicle), 'showinactivewarnings': "0",
                                                    'sortwarningsby': "active", 'updateWarnings': "false"}, timeout=10)
                        if req.status_code == requests.codes.ok:

                            try:
                                json_obj = json.loads(req.text)
                                req.close()
                                return json_obj
                            except json.decoder.JSONDecodeError as e:
                                print(f"JSON Decoder Error in th Query:{e}")
                                if logger is not None:
                                    logger.info("JSON Decode Error", exc_info=e, extra={'raw_data': req.text})
                                req.close()
                                session.cookies.clear()
                                # Invalidate Login
                                self.login_stat = False
                                return None
                else:
                    self.login_stat = False
                    return None
            else:
                self.login_stat = False
                return None

    def fetch_update(self):
        global logger
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        with self.obj_lock:
            if self.login_stat and self.login_timestamp is not None:
                if (self.login_timestamp - now) < datetime.timedelta(minutes=40):
                    try:
                        with self.session as session:
                            req = session.request(method='POST', url=self.url_update, headers=self.headers_update,
                                                  timeout=10)
                            if req.status_code == requests.codes.ok:

                                try:
                                    json_obj = json.loads(req.text)
                                    req.close()
                                    return json_obj
                                except json.decoder.JSONDecodeError as e:
                                    if logger is not None:
                                        logger.info("JSON Decode Error", exc_info=e, extra={'raw_data': req.text})
                                    print(f"JSON Decoder Error:{e}")
                                    req.close()
                                    session.cookies.clear()
                                    # Invalidate Login
                                    self.login_stat = False
                                    return None
                    except (requests.exceptions.ConnectionError, requests.exceptions.ConnectionError,
                            requests.exceptions.RetryError, requests.exceptions.RequestException) as e:

                        print(f"Request Error:{e}")
                        if logger is not None:
                            logger.info("Request Exception", exc_info=e)
                        self.login_timestamp = None
                        self.cookies = None
                        self.login_stat = False
                        session.cookies.clear()
                        return None
                else:
                    self.login_stat = False
                    return None
            else:
                self.login_stat = False
                return None

    def fetch_update_auto(self) -> Union[dict, None]:
        global logger
        with self.obj_lock:
            if self.login_stat:
                try:
                    return_val = self.fetch_update()
                except (requests.exceptions.ConnectionError, requests.exceptions.ConnectionError,
                        requests.exceptions.RetryError, requests.exceptions.RequestException) as e:
                    print(f"Request Error:{e}")
                    if logger is not None:
                        logger.info("Request Exception", exc_info=e)
                    self.login_timestamp = None
                    self.cookies = None
                    self.login_stat = False
                    return None
                if return_val is not None:
                    processed_obj = export_mem_object_th(return_val, datetime.datetime.now(tz=datetime.timezone.utc),
                                                             None)
                    self.vehicle_id_list.clear()
                    for key in processed_obj:
                        if str(key).isnumeric():
                            try:
                                id = int(key)
                                self.vehicle_id_list.append(id)
                            except ValueError as err:
                                print(f"Value error in fetch_update_auto:{err}")
                    return processed_obj
                elif not self.login_stat:
                    success = self.do_login()
                    if success:
                        try:
                            return_val = self.fetch_update()
                        except (requests.exceptions.ConnectionError, requests.exceptions.ConnectionError,
                                requests.exceptions.RetryError, requests.exceptions.RequestException) as e:
                            print(f"Request Error:{e}")
                            if logger is not None:
                                logger.info("Request Exception", exc_info=e)
                            self.login_timestamp = None
                            self.cookies = None
                            self.login_stat = False
                            return None
                        if return_val is not None:
                            try:
                                return_val = self.fetch_update()
                            except (requests.exceptions.ConnectionError, requests.exceptions.ConnectionError,
                                    requests.exceptions.RetryError, requests.exceptions.RequestException) as e:
                                print(f"Request Error:{e}")
                                if logger is not None:
                                    logger.info("Request Exception", exc_info=e)
                                self.login_timestamp = None
                                self.cookies = None
                                self.login_stat = False
                                return None
                            processed_obj = export_mem_object_th(return_val,
                                                                     datetime.datetime.now(tz=datetime.timezone.utc), None)
                            return processed_obj
                        return None
                    else:
                        return None
            else:
                success = self.do_login()
                if success:
                    try:
                        return_val = self.fetch_update()
                    except requests.exceptions.RequestException as e:
                        print(f"Request Error:{e}")
                        if logger is not None:
                            logger.exception("Request Exception", exc_info=e)
                        self.login_stat = False
                        self.login_timestamp = None
                        self.cookies = None
                        return None
                    if return_val is not None:
                        try:
                            return_val = self.fetch_update()
                        except requests.exceptions.RequestException as e:
                            print(f"Request Error:{e}")
                            if logger is not None:
                                logger.exception("Request Exception", exc_info=e)
                            self.login_timestamp = None
                            self.cookies = None
                            self.login_stat = False
                            return None
                        processed_obj = export_mem_object_th(return_val,
                                                                 datetime.datetime.now(tz=datetime.timezone.utc), None)
                        return processed_obj
                    return None
                else:
                    return None


class LogHandler:
    LOG_LVL_ERROR = 0
    LOG_LVL_WARNING = 1
    LOG_LVL_INFO = 2
    LOG_LVL_DEBUG = 3

    def __init__(self, log_file_lock_arg: RLock, log_file_path_arg: str, max_size: int = 1024 * 1024 * 20):
        self.file_lock = log_file_lock_arg
        self.log_file_path = log_file_path_arg
        self.max_file_size = max_size
        self.log_level = self.LOG_LVL_ERROR
        with self.file_lock:
            if pathlib.Path(self.log_file_path).is_file():
                pathlib.Path(self.log_file_path).unlink()

    def log_error(self, e: Exception):
        with self.file_lock:
            log_file_handle = open(self.log_file_path, "a+", encoding="utf-8")
            time_tmp = datetime.datetime.now(datetime.timezone.utc).strftime("%d_%M_%Y_%H-%M-%S")
            trace = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__, limit=10, chain=True)
            trace_j = "".join(trace)
            log_file_handle.write(
                f'{time_tmp} : Error occurred:\n {e} \nTraceback:\n{trace_j}')
            log_file_handle.close()

    def set_log_level(self, level: int):
        self.log_level = level % 4

    def log_warning(self, e: Exception, msg: str):
        if self.log_level >= LogHandler.LOG_LVL_WARNING:
            with self.file_lock:
                log_file_handle = open(self.log_file_path, "a+", encoding="utf-8")
                time_tmp = datetime.datetime.now(datetime.timezone.utc).strftime("%d_%M_%Y_%H-%M-%S")
                trace = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__, limit=10, chain=True)
                trace_j = "".join(trace)
                log_file_handle.write(
                    f'{time_tmp} : Warning occurred:\n {e} \nTraceback:\n{trace_j}\n Msg:{msg}')
                log_file_handle.close()


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    if obj is None:
        return "None"
    raise TypeError("Type %s not serializable" % type(obj))


class InternalConnectionError(RuntimeError):

    def __init__(self, message, status_code, error_obj, *args):
        self.message = message + f"\n Status Code:{status_code}"  # without this you may get DeprecationWarning
        # Special attribute you desire with your Error,
        # perhaps the value that caused the error?:
        self.status_code = status_code

        # allow users initialize misc. arguments as any other builtin Error
