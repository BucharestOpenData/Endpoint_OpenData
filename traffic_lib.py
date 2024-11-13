
import time
import uuid
from os import PathLike
from typing import Union
from urllib.parse import urlencode
import requests
import json
from json import JSONDecodeError
import pathlib
import queue
import mariadb
from urllib3.exceptions import DecodeError, InsecureRequestWarning
import re
import threading
import io
import os
import hashlib
import multiprocessing as mp
import logging
import logging.handlers
import warnings
import datetime
import numpy as np
import geojson
import urllib
import gc
import tracemalloc
import linecache
import copy

TRAFFIC_DATA_JSON_PREFIX = "raw_data_json_"

TRAFFIC_GEOJSON_PREFIX_STR = "data_geojson_"

TRAFFIC_THREAD_CONST = "_traffic_lib"

tracemalloc.start()
log_queue = queue.Queue(1024)
traffic_endpoint_url = ""




def display_top(snapshot, logger: logging.Logger, key_type='lineno', limit=10):
    snapshot = snapshot.filter_traces((
        tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
        tracemalloc.Filter(False, "<unknown>"),
    ))
    top_stats = snapshot.statistics(key_type)
    object_list = list()
    logger.info("Top %s lines", limit)
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        logger.info(f"#%s: %s:%s: %.1f KiB",
                    index, frame.filename, frame.lineno, stat.size / 1024)
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            logger.info('    %s', line)
        if isinstance(frame, object):
            object_list.append(frame)
    unique_objects = list(set(object_list))
    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        logger.info("%s other: %.1f KiB", len(other), size / 1024)
    total = sum(stat.size for stat in top_stats)
    logger.info("Total allocated size: %.1f KiB", (total / 1024))
    return unique_objects

def is_valid_json(json_string:Union[str,None])->bool:
    if json_string is None:
        return False
    if not isinstance(json_string, str):
        return False
    try:
        json_object = json.loads(json_string)
    except json.JSONDecodeError as e:
        return False
    del json_object
    return True
def is_valid_uuid(uuid_string:str)->bool:
    if uuid_string is None:
        return False
    if not uuid_string.isalnum() :
        return False
    try:
        val = uuid.UUID(uuid_string, version=4)
        return str(val) == uuid_string
    except ValueError:
        return False
def uuid_to_int(uuid_string:str)->int:
    if uuid_string is None:
        return -1
    if uuid_string == '' :
        return -1
    try:
        uuid_obj = uuid.UUID(uuid_string)
        return copy.copy(uuid_obj.int)
    except ValueError:
        return -1

def str_to_bool(s)->bool:
    true_values = {'true', '1', 'yes', 'y', 't'}
    false_values = {'false', '0', 'no', 'n', 'f'}
    if s is None or not isinstance(s,str):
        raise ValueError(f"Cannot convert {s} to boolean")
    if s.strip().lower() in true_values:
        return True
    elif s.strip().lower() in false_values:
        return False
    else:
        raise ValueError(f"Cannot convert {s} to boolean")

def setup_logger(queue_arg: mp.Queue, log_level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(mp.current_process().name + TRAFFIC_THREAD_CONST)
    logger.setLevel(log_level)
    handler_queue = logging.handlers.QueueHandler(queue_arg)
    formatter = logging.Formatter('[%(asctime)s] {PID:p%(process)s} {Process:%(processName)s} {Thread_id:%('
                                  'thread)d}  {%(threadName)s:%('
                                  'funcName)s}  {%(pathname)s:%(lineno)d}  %(levelname)s - %(message)s',
                                  '%d-%m-%Y %H:%M:%S')
    handler_queue.setFormatter(formatter)
    logger.addHandler(handler_queue)
    logger.propagate = False

    return logger


def traffic_data_disk_writer(queue_geojson: mp.Queue, queue_traffic_json: mp.Queue, directory: PathLike,
                             sig_event: mp.Event) -> int:
    path_full = pathlib.Path(directory).resolve()
    if not path_full.is_dir():
        path_full.mkdir(parents=True, exist_ok=True, mode=0o775)
    filename_geojson_prefix = TRAFFIC_GEOJSON_PREFIX_STR
    filename_raw_json_prefix = TRAFFIC_DATA_JSON_PREFIX
    thread_logger = setup_logger(log_queue)
    k_geojson = 0
    k_json = 0
    while not sig_event.is_set():
        try:
            entries = os.listdir(path_full)
            now = datetime.datetime.now(tz=datetime.timezone.utc)

            files = [path_full.joinpath(file).resolve() for file in entries if
                     os.path.isfile(os.path.join(path_full, file))]
            escaped_prefix_geojson = re.escape(filename_geojson_prefix)
            regex_traffic_geojson = re.compile(
                rf'{escaped_prefix_geojson}(\d{{4}}-\d{{2}}-\d{{2}}-\d{{2}}-\d{{2}}-\d{{2}})_\d+\.geojson')
            creation_time_list = list()
            if len(files) > 100:
                for file in files:
                    file_name_stem = file.stem
                    try:
                        match = re.match(regex_traffic_geojson, file_name_stem)
                        if match:
                            time_str = match.group(1)
                            creation_time = datetime.datetime.strptime(time_str, "%Y-%m-%d-%H-%M-%S")
                            creation_time_list.append((file, creation_time))
                    except (TypeError, ValueError) as e:
                        thread_logger.warning("Error for file %s", file_name_stem, exc_info=e)

                newest = creation_time_list[0][1]
                newest_file = creation_time_list[0][0]
                for entry in creation_time_list:
                    if entry[1] > newest:
                        newest = entry[1]
                        newest_file = entry[0]
                for file in files:
                    if file != newest_file:
                        file.unlink(missing_ok=True)

        except OSError as e:
            thread_logger.warning("OS Error listing directory", exc_info=e)

        try:
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            str_time = now.strftime("%Y-%m-%d-%H-%M-%S")
            while not queue_geojson.empty():
                geojson_obj = queue_geojson.get(block=True, timeout=1)

                file_name_full = filename_geojson_prefix + str_time + f"_{str(k_geojson)}" + ".geojson"
                file_path = path_full.joinpath(file_name_full)
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(str(geojson_obj))
                k_geojson += 1

        except queue.Empty:
            pass

        except PermissionError as e:
            thread_logger.warning("Permission Error saving files", exc_info=e)
        except OSError as e:
            thread_logger.warning("OS Error saving files", exc_info=e)
        try:
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            str_time = now.strftime("%Y-%m-%d_%H-%M-%S")
            while not queue_traffic_json.empty():
                traffic_json_obj = queue_traffic_json.get(block=True, timeout=1)
                file_name_full = filename_raw_json_prefix + str_time + f"_{str(k_json)}" + ".json"
                file_path = path_full.joinpath(file_name_full)
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(str(traffic_json_obj))
                k_json += 1

        except queue.Empty:

            pass
        time.sleep(1)
    return 0


class TrafficLibClass:
    """
    Initializes the TrafficLibClass object with the given database and URL parameters.

    Parameters:
        url (str): The Traffic API URL.
        db_username (str): Database username for authentication.
        db_passwd (str): Database password for authentication.
        db_name (str): Name of the database to connect to.
        host (str): Host address of the database. Defaults to "127.0.0.1".
        port (int): Port number for the database connection. Defaults to 3306.

    Raises:
        ValueError: If the provided URL is None.
    """
    def __init__(self, url: str, db_username: str, db_passwd: str, db_name: str, host: str = "127.0.0.1",
                 port: int = 3306):
        global log_queue
        if url is None:
            raise ValueError("url is None")
        parsed_url = urllib.parse.urlparse(url)
        query_dict = urllib.parse.parse_qs(parsed_url.query)
        if "format" in query_dict:
            del query_dict["format"]
        new_query = urlencode(query_dict, doseq=True)
        new_url = urllib.parse.urlunparse((
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            parsed_url.params,
            new_query,
            parsed_url.fragment
        ))

        self.url_traffic_info = new_url
        self.obj_lock = threading.RLock()
        self.logger = setup_logger(log_queue)
        self.traffic_jam_list: list[Union[dict, list]] = list()
        self.db_user: str = str(db_username)
        self.db_password: str = db_passwd
        self.db_name: str = str(db_name)
        self.host_str: str = str(host)
        self.port: int = int(port)
        self.connection = None
        self.traffic_json_object_queue = queue.Queue(maxsize=1024)
        self.traffic_geojson_jams_object_queue = queue.Queue(maxsize=1024)
        self.latest_geojson_jams_fifo_list: Union[list[tuple[str, datetime.datetime]], None] = list()
        self.latest_geojson_alerts_fifo_list:Union[list[tuple[str, datetime.datetime]], None] = list()
        self.geo_json_fifo_size = 10
        self.db_archive_cache: list = list()
        self.db_archive_cache_update_time = None
        self.connection_lock = threading.RLock()
        self.enable_queue_write = False

    def empty_cache(self) -> Union[int, None]:
        """
        Clears the database archive cache and resets the cache update time.

        Return:
            The size of the cache before it was cleared, or None if an error occurs.
        """
        try:
            size_cache: int = 0
            with self.obj_lock:
                size_cache = len(self.db_archive_cache)
                self.db_archive_cache.clear()
                self.db_archive_cache_update_time = None
            return size_cache
        except (ValueError, IndexError):
            return None

    def empty_queues(self) -> bool:
        """
        Checks and empties the Traffic JSON and GeoJSON object queues.

        Returns:
            bool: True if both the Traffic JSON and GeoJSON object queues are empty after attempting to clear them; False otherwise.

        Exceptions:
            queue.Empty: Caught if an attempt is made to get an item from an empty queue.
        """
        try:
            while not self.traffic_json_object_queue.empty():
                self.traffic_json_object_queue.get_nowait()
                self.traffic_json_object_queue.task_done()
        except queue.Empty:
            pass
        try:
            while not self.traffic_geojson_jams_object_queue.empty():
                self.traffic_geojson_jams_object_queue.get_nowait()
                self.traffic_geojson_jams_object_queue.task_done()
        except queue.Empty:
            pass

        return self.traffic_geojson_jams_object_queue.empty() and self.traffic_json_object_queue.empty()

    def get_latest_geojson_jams(self) -> Union[tuple[str, datetime.datetime], None]:
        """
        Retrieves the most recent GeoJSON data from a FIFO list.

        Returns:
            A tuple containing the latest GeoJSON string and a datetime object if available, otherwise None.
        """
        with self.obj_lock:
            ret_val = (self.latest_geojson_jams_fifo_list[-1]) if len(self.latest_geojson_jams_fifo_list) > 0 else None
        return ret_val

    def get_latest_geojson_alerts_serialized(self) -> Union[tuple[str,str],None]:
        """
        Returns the latest geojson alerts serialized

        Acquires a lock to ensure thread-safe access to the latest geojson alerts list.
        If there are any alerts, it creates a deep copy of the latest entry in the list.
        If the list is empty, returns None.
        If an entry exists, it returns a tuple containing a geojson string and an ISO formatted date-time string.

        Returns:
            Tuple containing geojson string and ISO formatted date-time string, or None if no alerts are present.
        """
        tuple_data = None
        with (self.obj_lock):
            tuple_data = copy.deepcopy(self.latest_geojson_alerts_fifo_list[-1]) if len(
                self.latest_geojson_alerts_fifo_list) > 0 else None
        if tuple_data is None:
            return None
        else:
            geo_str = tuple_data[0]
            time_date_str = tuple_data[1].isoformat()
            return geo_str, time_date_str
    def get_latest_geojson_jams_serialized(self) -> Union[tuple[str, str], None]:
        """

        Returns the latest geojson data and corresponding timestamp in ISO format, if available.

        The method fetches the latest entry from a FIFO list that stores geojson data and timestamps.
        It uses a thread lock to ensure thread safety during the read operation.

        Returns:
            A tuple containing the latest geojson data as a string and the corresponding timestamp in ISO format.
            None if the FIFO list is empty.
        """
        tuple_data = None
        with self.obj_lock:
            tuple_data = copy.deepcopy(self.latest_geojson_jams_fifo_list[-1]) if len(
                self.latest_geojson_jams_fifo_list) > 0 else None
        if tuple_data is None:
            return None
        else:
            geo_str: str = tuple_data[0]
            time_date_str: str = tuple_data[1].isoformat()
            return geo_str, time_date_str

    def get_db_archive_list_by_date(self, record_limit: int = None, interval_start: datetime.datetime = None,
                                    interval_end: datetime.datetime = None) -> Union[
        list[tuple[int, datetime.datetime,int]], None]:
        """
        Fetches a list of database archive records within a specified date interval.

        Parameters:
            record_limit (int, optional): The maximum number of records to return. Defaults to 65,535 if not specified.
            interval_start (datetime.datetime, optional): The start of the date interval. Defaults to Jan 1, 1970, UTC if not specified.
            interval_end (datetime.datetime, optional): The end of the date interval. Defaults to current UTC datetime if not specified.

        Returns:
            list of tuples (int, datetime.datetime) or None:
            A list of tuples where each tuple contains an integer ID and a UTC datetime timestamp.
            Returns None if there is no database connection or if an error occurs during the query execution.

        Description:
            This function retrieves records from the 'geojson_archive' table within the specified date
            interval. If the date interval is not provided, it defaults to a wide range from Jan 1, 1970,
            to the current UTC datetime. The results are ordered by timestamp in descending order and limited by
            the 'record_limit' parameter. The function also handles various types of errors and ensures
            that appropriate connections and locks are managed during the process. If records are retrieved,
            it updates an internal cache with a portion of the results.
        """
        # create ceiling if not given
        if record_limit is None:
            record_limit = 2 ** 16 - 1

        query_sql = (
            "SELECT id,timestamp_db,type from traffic_db.geojson_archive WHERE timestamp_db BETWEEN %s AND %s ORDER BY timestamp_db DESC LIMIT "
            "%s ;")

        if not self.chk_connection():
            self.logger.warning("No DB connection")
            return None
        if interval_start is None or not isinstance(interval_start, datetime.datetime):
            interval_start = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        if interval_end is None or not isinstance(interval_end, datetime.datetime):
            interval_end = datetime.datetime.now(tz=datetime.timezone.utc)

        if interval_start > interval_end:
            interval_start, interval_end = interval_end, interval_start

        interval_start_str = interval_start.strftime("%Y-%m-%d %H:%M:%S")
        interval_end_str = interval_end.strftime("%Y-%m-%d %H:%M:%S")
        result_list = list()
        try:
            with self.connection_lock:
                self.connection.begin()
                cursor = self.connection.cursor(buffered=True)
                try:
                    cursor.execute(query_sql, (interval_start_str, interval_end_str, record_limit))

                    for row in cursor:
                        try:
                            ts = row[1]
                            id_traffic_db = int(row[0])
                            type_geojson = int(row[2])
                            timestamp_db = None
                            if isinstance(ts, datetime.datetime):
                                timestamp_db = ts.replace(tzinfo=datetime.timezone.utc)
                            elif isinstance(ts, str):
                                try:
                                    timestamp_db = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(
                                        tzinfo=datetime.timezone.utc)
                                except ValueError:
                                    timestamp_db = None
                            else:
                                timestamp_db = None
                            result_list.append((id_traffic_db, timestamp_db,type_geojson))

                        except (ValueError, IndexError) as e:
                            continue

                    self.connection.commit()




                except (mariadb.ProgrammingError,mariadb.OperationalError) as e:
                    self.logger.warning("Get Archive List DB Programming Error", exc_info=e)
                    self.connection.rollback()
                    return None
                finally:
                    cursor.close()
                    del cursor

            if len(result_list) > 0:
                with self.obj_lock:
                    self.db_archive_cache.clear()
                    self.db_archive_cache.extend(result_list[:1024])
                    self.db_archive_cache_update_time = datetime.datetime.now(tz=datetime.timezone.utc)
            return result_list
        except mariadb.Error as e:
            self.logger.warning("Get Archive List Error DB", exc_info=e)
            return None

    def get_db_archive_list(self, record_limit: int = 4096) -> Union[list[tuple[int, datetime.datetime,int]], None]:
        """
        Retrieve a list of database archive records with specified limits.

        Parameters:
        record_limit (int): Maximum number of records to retrieve. Defaults to 4096.
                             If set to a value less than 1, all records will be retrieved.

        Returns:
        list[tuple[int, datetime.datetime]]: List of tuples, each containing an archive ID and a timestamp.
        None: Returns None if there is no database connection or in case of an error.

        Behavior:
        - Constructs SQL query to fetch a list of IDs and timestamps from the 'geojson_archive' table.
        - If the database connection check fails, logs a warning message and returns None.
        - Executes the SQL query within a locked database connection context.
        - Processes each row in the result set, converting the timestamp to a UTC datetime object and appending to the result list.
        - Catches and continues on ValueError or IndexError encountered during row processing.
        - Commits the transaction and updates the internal cache.
        - Logs and returns None in case of database-related errors.

        Exceptions Handling:
        - Catches mariadb.ProgrammingError and logs a warning message.
        - Rolls back the transaction if a programming error occurs.
        - Catches general mariadb.Error and logs a warning message.
        """
        if record_limit < 1:
            sql_query_statement = f"SELECT id,timestamp_db,type from traffic_db.geojson_archive ORDER BY timestamp_db DESC;"
        else:
            sql_query_statement = f"SELECT id,timestamp_db,type from traffic_db.geojson_archive ORDER BY timestamp_db DESC LIMIT{int(record_limit)};"
        if not self.chk_connection():
            self.logger.warning("No DB connection")
            return None
        try:
            with self.connection_lock:
                self.connection.begin()
                cursor = self.connection.cursor(buffered=True)
                try:

                    cursor.execute(sql_query_statement)
                    result_list = list()

                    for row in cursor:
                        try:
                            db_archive_id = int(row[0]) if row[0] is not None else None
                            ts_data = row[1]
                            type_geo = row[2]
                            if db_archive_id is None or ts_data is None:
                                continue
                            if isinstance(ts_data, datetime.datetime):
                                ts = ts_data.replace(tzinfo=datetime.timezone.utc)
                            else:
                                ts = datetime.datetime.strptime(ts_data, "%Y-%m-%d %H:%M:%S").replace(
                                    tzinfo=datetime.timezone.utc)
                            result_list.append((db_archive_id, ts,type_geo))
                        except (ValueError, IndexError) as e:
                            continue

                    self.connection.commit()
                    with self.obj_lock:
                        self.db_archive_cache.clear()
                        self.db_archive_cache.extend(result_list[:1024])
                        self.db_archive_cache_update_time = datetime.datetime.now(tz=datetime.timezone.utc)
                    return result_list
                except (mariadb.ProgrammingError,mariadb.OperationalError) as e:
                    self.logger.warning("Get Archive List DB Programming Error", exc_info=e)

                    self.connection.rollback()
                    return None
                finally:
                    cursor.close()
                    del cursor
        except mariadb.Error as e:
            self.logger.warning("Get Archive List Error DB", exc_info=e)
            return None

    def get_db_archive_geo_json_multiple(self, id_archive: list[int]) -> Union[
        None, list[tuple[str, datetime.datetime,int]]]:
        """
        Retrieve GeoJSON data and timestamps from the database for multiple archive IDs.

        Args:
            id_archive (list[int]): List of archive IDs to retrieve data for.

        Returns:
            Union[None, list[tuple[str, datetime.datetime]]]: A list of tuples where each tuple contains
            a GeoJSON string and a datetime object. Returns None if there is no database connection,
            if the input parameter is not a list, if the input list is empty, or if any errors occur during
            the database queries.

        Logs:
            Warnings are logged for missing database connections, incorrect input types, empty input lists,
            and any database errors that occur.

        Raises:
            mariadb.ProgrammingError: If there is a programming error during SQL execution.
            mariadb.Error: If there is a general database error.
        """
        sql_statement = f"SELECT geojson_data,timestamp_db,type FROM traffic_db.geojson_archive WHERE id = %s;"
        if not self.chk_connection():
            self.logger.warning("No DB connection")
            return None
        if not isinstance(id_archive, list):
            self.logger.warning("Incorrect input parameter.List required")
            return None
        if len(id_archive) == 0:
            self.logger.debug("Empty array in")
            return None
        try:
            with self.connection_lock:
                self.connection.begin()
                cursor = self.connection.cursor(buffered=True)
                try:
                    result_list = list()
                    for i in id_archive:
                        cursor.execute(sql_statement, (i,))

                        results_sql = cursor.fetchall()
                        for row in results_sql:
                            timestamp = row[1]
                            type_geo = int(row[2])
                            if isinstance(timestamp, datetime.datetime):
                                timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
                            elif isinstance(timestamp, str):
                                timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").replace(
                                    tzinfo=datetime.timezone.utc)
                            else:
                                timestamp = None
                                continue
                            result_list.append((str(row[0]), timestamp,type_geo))

                    self.connection.commit()
                    if len(result_list) == 0:
                        self.logger.warning(f"No entries found")
                        return None

                    else:
                        return result_list

                except (mariadb.ProgrammingError,mariadb.OperationalError) as e:
                    self.logger.warning("Get Archive List DB Programming Error", exc_info=e)

                    self.connection.rollback()
                    return None
                finally:
                    cursor.close()
                    del cursor
        except mariadb.Error as e:
            self.logger.warning("Get Archive List Error DB", exc_info=e)
            return None

    def get_db_archive_geo_json(self, id_archive: int) -> Union[None, tuple[str, datetime.datetime,int]]:
        """
        Fetches GeoJSON data and its associated timestamp from a database archive.

        This function retrieves the GeoJSON data and timestamp for a given archive ID
        from the 'geojson_archive' table in the 'traffic_db' database. It returns a tuple
        containing the GeoJSON data as a string and the timestamp as a datetime object
        with UTC timezone.

        Parameters:
        id_archive (int): The ID of the archive entry to be fetched.

        Returns:
        Union[None, tuple[str, datetime.datetime]]: A tuple with GeoJSON data and timestamp
        in UTC if found. Returns None if no entry is found, there are multiple entries,
        or any database error occurs.

        Warnings:
        Generates warnings when there is no database connection, if no or multiple entries
        are found for the given ID, or if any database programming errors occur.
        """
        sql_statement = f"SELECT geojson_data,timestamp_db,type FROM traffic_db.geojson_archive WHERE id = %s;"
        if not self.chk_connection():
            self.logger.warning("No DB connection")
            return None
        try:
            with self.connection_lock:
                self.connection.begin()
                cursor = self.connection.cursor(buffered=True)
                try:
                    cursor.execute(sql_statement, (id_archive,))
                    result_list = list()

                    for row in cursor:

                        timestamp = row[1]
                        geo_type = int(row[2])
                        if isinstance(timestamp, datetime.datetime):
                            timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
                        elif isinstance(timestamp, str):
                            timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").replace(
                                tzinfo=datetime.timezone.utc)
                        else:
                            timestamp = None
                            continue
                        result_list.append((str(row[0]), timestamp,geo_type))

                    self.connection.commit()
                    if len(result_list) == 0:
                        self.logger.warning(f"No entry found for id:{id_archive}")
                        return None
                    elif len(result_list) > 1:
                        self.logger.warning(f"Multiple entries found for id:{id_archive}")
                        return result_list.pop()
                    else:
                        return result_list.pop()

                except (mariadb.ProgrammingError,mariadb.OperationalError) as e:
                    self.logger.warning("Get Archive List DB Programming Error", exc_info=e)

                    self.connection.rollback()
                    return None
                finally:
                    cursor.close()
                    del cursor
        except mariadb.Error as e:
            self.logger.warning("Get Archive List Error DB", exc_info=e)
            return None

    def connect_to_db(self) -> Union[mariadb.Connection, None]:
        """

        Connects to the MariaDB database using the given credentials and configuration.

        Tries to establish a new connection to the database. If an existing connection is open,
        it closes it before attempting to connect again.

        Returns:
            mariadb.Connection: The established database connection if successful.
            None: If the connection attempt fails, returns None.
        """
        with self.connection_lock:
            try:
                if self.connection is not None:
                    self.connection.close()
                connection = mariadb.connect(
                    user=self.db_user,
                    password=self.db_password,
                    host=self.host_str,
                    port=self.port,
                    database=self.db_name,
                    autocommit=False
                )
                self.connection = connection
                return connection
            except mariadb.Error as e:
                self.logger.warning("Connect Error", exc_info=e)
                self.connection = None
                return None

    def chk_connection(self) -> bool:
        """
        Checks the status of the database connection.

        Returns:
            bool: True if the connection is alive, False otherwise.
        """
        with self.connection_lock:
            if self.connection is None:
                return False
            else:
                try:
                    self.connection.ping()
                    return True
                except mariadb.Error as e:
                    self.logger.warning("Ping Error", exc_info=e)
                    return False

    def close_connection(self) -> None:
        if self.chk_connection():
            with self.connection_lock:
                self.connection.close()
                self.connection = None
        else:
            with self.connection_lock:
                self.connection = None

    def refresh_connection(self)->Union[None,mariadb.Connection]:
        self.close_connection()
        connection = self.connect_to_db()
        if self.chk_connection():
            return connection
        else:
            self.close_connection()
            return None
    def download_json(self) -> Union[tuple[Union[dict, str], str], tuple[Union[list, str], str], None]:
        """
        Downloads and processes JSON data from the given URL.

        Returns a tuple containing JSON data and its string representation,
        or None in case of a request failure.

        Returns:
            Union[tuple[Union[dict, str], str], tuple[Union[list, str], str], None]:
            A tuple with the JSON data and its string representation, or None in case of a request failure.

        Raises:
            requests.exceptions.InvalidURL: If the URL provided is invalid.
            requests.exceptions.ReadTimeout: If the request times out.
            requests.exceptions.RetryError: If the request fails after retries.
            requests.exceptions.ConnectTimeout: If the request connection times out.
            requests.exceptions.ConnectionError: If the request encounters a connection error.
            requests.exceptions.InvalidJSONError: If the JSON is invalid or cannot be decoded.
        """
        with self.obj_lock:
            url_tmp = copy.deepcopy(self.url_traffic_info)
            buffer = io.BytesIO()
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            warnings_list = list()
            return_val:str = ""
            string_val:str = ""
            ok = True
            try:

                with requests.Session() as session:
                    with warnings.catch_warnings(record=True) as w:
                        warnings.simplefilter("default")
                        warnings.filterwarnings("ignore", category=DeprecationWarning)
                        warnings.filterwarnings("ignore", category=PendingDeprecationWarning)
                        warnings.filterwarnings("ignore", category=InsecureRequestWarning)

                        response = session.get(url_tmp, params={"format": 1}, stream=True)

                        if response.status_code == requests.codes.ok:
                            for chunk in response.iter_content(chunk_size=1024):
                                buffer.write(chunk)
                            buffer.seek(0)



                            try:
                                string_val = buffer.getvalue().decode("utf-8")
                                return_val = json.loads(string_val)
                            except UnicodeDecodeError as e:
                                ok = False
                                buffer.seek(0)



                        warnings_list.extend(w)

                for w in warnings_list:
                    self.logger.info("Traffic_Lib request JSON Warning Msg: %s  "
                                     "Category: %s  "
                                     "FileName:%s  "
                                     "line_nr:%s  line: %s",
                                     str(w.message),
                                     str(w.category.__name__),
                                     str(w.filename),
                                     str(w.lineno), str(w.line))

                return return_val, string_val

            except (requests.exceptions.InvalidURL, requests.exceptions.ReadTimeout, requests.exceptions.RetryError,
                    requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError,
                    requests.exceptions.InvalidJSONError) as e:
                self.logger.warning("Traffic Lib Exception in JSON HTTP Request", exc_info=e)
                return None
            finally:
                buffer.close()

    def store_json_raw(self, traffic_obj_tuple: tuple[object, str], timestamp: Union[datetime.datetime, None] = None) -> \
            Union[tuple[int, bool], None]:
        """
        Stores a Traffic JSON object into a database and optionally adds it to a queue.

        Args:
            traffic_obj_tuple (tuple[object, str]): A tuple containing the Traffic object and its JSON string representation.
            timestamp (Union[datetime.datetime, None]): The timestamp when the data is being stored. Defaults to the current time if None.

        Returns:
            Union[tuple[int, bool], None]: Returns a tuple containing the database ID and a boolean indicating whether the record already existed, or None in case of failures.

        The function performs the following operations:
        - Checks for valid database connection.
        - Validates the input tuple.
        - Generates a SHA-256 hash of the JSON string.
        - Checks if the hash already exists in the database.
        - Inserts the JSON data and its hash into the database if it doesn't already exist.
        - Optionally adds the JSON string to a queue if `self.enable_queue_write` is set to True.
        """
        statement_sql_insert = f"INSERT INTO traffic_db.tblTraffic_upload_raw(json_data,timestamp,hash_hex) VALUES (%s,%s,%s);"
        statement_sql_query = f"SELECT id from traffic_db.tblTraffic_upload_raw WHERE hash_hex = %s LIMIT 1;"
        statement_sql_query_hash = f"Select id from traffic_db.tblTraffic_upload_raw WHERE hash_hex = %s ;"
        timestamp_str_db = timestamp.strftime("%Y-%m-%d %H:%M:%S") if timestamp is not None \
            else datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        if not self.chk_connection():
            self.logger.warning("No DB connection")
            return None
        if traffic_obj_tuple is None:
            self.logger.warning("No input object tuple")
            return None
        if traffic_obj_tuple[0] is None or traffic_obj_tuple[1] is None:
            self.logger.warning("Input tuple is invalid")
            return None
        sha256_hex = None
        json_str = None
        try:
            json_str = traffic_obj_tuple[1]
            if not is_valid_json(json_str):
                self.logger.warning("Invalid JSON string: %s", str(json_str))
                return None
            byte_str = json_str.encode("utf-8", errors="replace")
            hash_object = hashlib.sha256()
            hash_object.update(byte_str)
            sha256_hex = hash_object.hexdigest().upper()

            try:
                if self.enable_queue_write:
                    self.traffic_json_object_queue.put(json_str, block=True, timeout=0.1)
            except queue.Full as e:
                self.logger.warning("Traffic Object Queue Full", exc_info=e)

        except (TypeError, UnicodeEncodeError) as e:
            self.logger.warning("Encoder JSON Error", exc_info=e)
            return None
        if json_str is None or sha256_hex is None:
            self.logger.warning("None JSON and HASH values")
            return None
        try:
            exists_in_db = False
            with self.connection_lock:
                cursor = self.connection.cursor(buffered=True)
                self.connection.begin()
                try:
                    # check if hash exists in db
                    cursor.execute(statement_sql_query_hash, (sha256_hex,))
                    res = cursor.fetchall()
                    id_db = None
                    if res is not None and len(res) > 0:
                        id_db = res[0][0]
                        exists_in_db = True
                    else:

                        cursor.execute(statement_sql_insert, (json_str, timestamp_str_db, sha256_hex))
                        self.connection.commit()
                        cursor.execute(statement_sql_query, (sha256_hex,))
                        res = cursor.fetchall()
                        if res is not None and len(res) > 0:
                            id_db = res[0][0]
                        else:
                            id_db = None

                    return id_db, exists_in_db
                except mariadb.ProgrammingError as e:
                    self.logger.info("Sql Programming Error", exc_info=e)

                    self.connection.rollback()
                    return None
                except mariadb.OperationalError as e:
                    self.logger.info("Sql Operational Error", exc_info=e)
                    self.connection.rollback()
                    return None
                except mariadb.IntegrityError as e:
                    self.logger.info("Sql Integrity Error", exc_info=e)
                    self.connection.rollback()
                    return None
                finally:
                    cursor.close()
                    cursor.__del__()

        except mariadb.Error as e:
            self.logger.warning("Store JSON Error", exc_info=e)
            return None

    def parse_json_data_jams(self, traffic_db_id: int, object_traffic: Union[object, list, dict, None]) -> Union[str, None]:
        """

        Parses the input Traffic JSON data for traffic jams and performs database operations.

        Parameters:
        traffic_db_id (int): Identifier for the Traffic database.
        object_traffic (Union[object, list, dict, None]): The Traffic JSON data to be parsed.

        Returns:
        Union[str, None]: The resulting GeoJSON string if successful, or None if not.

        The method performs the following steps:
        1. Constructs SQL insert statements for inserting data into related tables.
        2. Checks for valid input data and database connection.
        3. Extracts traffic jams data from the provided JSON and processes each jam.
        4. Compiles relevant information (e.g., speed, location, delay) from the jams.
        5. Generates random variations for speeds and delays using a Gaussian distribution.
        6. Creates GeoJSON features for the processed traffic jams.
        7. Inserts the processed data into the database, handling potential errors.
        8. Caches the generated GeoJSON and enqueues it if configured.
        9. Returns the final GeoJSON string or None based on the processing outcome.

        Exception Handling:
        - Logs and counts errors encountered during data extraction.
        - Rolls back database operations on SQL error.
        """
        statement_insert = (f"INSERT INTO traffic_db.tblTrafficTrafficJams("
                            f"uuid,speed_ms,speed_kph,"
                            f"length,road_type,"
                            f"delay_traffic,street,city,country,"
                            f"segement_data,line_data,"
                            f"endNonde_name,startNode_name,"
                            f"level,turnLine_lat,turnLine_lon,"
                            f"turn_type,fk_raw_data,traffic_timestamp_ms,traffic_timestamp) VALUES("
                            f"%s,%s,%s,%s,%s,%s,"
                            f"%s,%s,%s,%s,%s,%s,"
                            f"%s,%s,%s,%s,%s,%s,%s,%s);")
        statement_insert_geojson = (f"INSERT INTO traffic_db.geojson_archive(geojson_data, fk_raw_data,type) "
                                    f"VALUES (%s,%s,0);")
        if object_traffic is None or (not isinstance(object_traffic, dict)) or traffic_db_id is None:
            return None
        if not self.chk_connection():
            return None
        jams_list = list()
        if "jams" in object_traffic:
            jams_list.extend(object_traffic["jams"])
        else:
            return None
        err_cnt = 0
        features_geojson_list = list()
        upload_data_list = list()
        traffic_db_id = int(traffic_db_id)
        for i_obj in jams_list:

            try:

                country = str(i_obj["country"])[:15] if "country" in i_obj else None
                city = str(i_obj["city"])[:31] if "city" in i_obj else None
                street = str(i_obj["street"])[:127] if "street" in i_obj else None
                line_list = list()
                segments = list()
                if "line" in i_obj:
                    line_list.extend(i_obj["line"])
                speed_km = float(i_obj["speedKMH"]) if "speedKMH" in i_obj else None
                length_m = float(i_obj["length"]) if "length" in i_obj else None
                turn_type = str(i_obj["turnType"])[:8] if "turnType" in i_obj else None
                if turn_type is not None:
                    if turn_type.lower() == "none":
                        turn_type = None
                turn_line = (i_obj["turn_line"]["x"], i_obj["turn_line"]["y"]) if "turn_line" in i_obj else (None, None)
                uuid = int(i_obj["uuid"]) if "uuid" in i_obj else None
                speed_m = float(i_obj["speed"]) if "speed" in i_obj else None
                if "segments" in i_obj:
                    segments.extend(i_obj["segments"])
                road_type = str(i_obj["roadType"]) if "roadType" in i_obj else None
                delay = int(i_obj["delay"]) if "delay" in i_obj else None
                id_jam = int(i_obj["id"]) if "id" in i_obj else None
                endNodeID = str(i_obj["endNode"])[:63] if "endNode" in i_obj else None
                startNodeID = str(i_obj["startNode"])[:63] if "startNode" in i_obj else None
                level = int(i_obj["level"]) if "level" in i_obj else None
                try:
                    timestamp_ms = int(i_obj["pubMillis"]) if "pubMillis" in i_obj else None
                    timestamp = datetime.datetime.fromtimestamp(
                        int(i_obj["pubMillis"]) / 1000) if "pubMillis" in i_obj else None
                    timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError as e:
                    timestamp = None
                    timestamp_ms = None
                    timestamp_str = None

                try:
                    entry = (uuid, speed_m, speed_km,
                             length_m, road_type,
                             delay, street, city, country,
                             json.dumps(segments, indent=2),
                             json.dumps(line_list, indent=2),
                             endNodeID, startNodeID, level,
                             turn_line[1], turn_line[0], turn_type,
                             traffic_db_id, timestamp_ms, timestamp_str
                             )
                    upload_data_list.append(entry)
                except (JSONDecodeError, UnicodeEncodeError) as e:
                    pass

                if delay is not None:
                    # Gaussian random number with mean equal to delay and std_dev 5%
                    std_dev_delay = delay * 0.01 if delay >= 0 else 0
                    mean_delay = delay
                    random_gauss_delay = np.random.normal(mean_delay, std_dev_delay, 6)[3]
                else:
                    random_gauss_delay = None
                if speed_m is not None:
                    std_dev_speed_m = speed_m * 0.01 if speed_m >= 0 else 0
                    mean_speed_m = speed_m
                    random_gauss_speed_m = np.random.normal(mean_speed_m, std_dev_speed_m, 6)[3]
                else:
                    random_gauss_speed_m = None
                if speed_km is not None:
                    std_dev_speed_km = speed_km * 0.01 if speed_km >= 0 else 0
                    mean_speed_km = speed_km
                    random_gauss_speed_km = np.random.normal(mean_speed_km, std_dev_speed_km, 6)[3]
                else:
                    random_gauss_speed_km = None
                properties_feature = {
                    "speed_ms": random_gauss_speed_m if speed_m > 0 else speed_m,
                    "speed_kph": random_gauss_speed_km if speed_km > 0 else speed_km,
                    "length": length_m,
                    "delay_s": random_gauss_delay if delay > 0 else delay,
                    "street": street,
                    "start_node": startNodeID,
                    "end_node": endNodeID
                }

                if len(line_list) >= 2:
                    point_data_list = list()
                    for elem in line_list:
                        tuple_p = float(elem["x"]), float(elem["y"])
                        point_data_list.append(tuple_p)
                    line_string = geojson.LineString(point_data_list)
                    feature_t = geojson.Feature(id=uuid, geometry=line_string, properties=properties_feature or {})
                    features_geojson_list.append(feature_t)
                if turn_type is not None and turn_line is not None:
                    point_geojson = geojson.Point(turn_line)
                    feature_p = geojson.Feature(id=uuid, geometry=point_geojson, properties=properties_feature or {})
                    features_geojson_list.append(feature_p)


            except (KeyError, IndexError, ValueError) as e:
                self.logger.warning(f"Error occurred for elem:{json.dumps(i_obj)}", exc_info=e)
                err_cnt += 1

        if len(upload_data_list) > 0 and self.chk_connection():
            try:
                with self.connection_lock:
                    cursor = self.connection.cursor(buffered=True)
                    self.connection.begin()
                    try:
                        cursor.executemany(statement_insert, upload_data_list)
                        self.connection.commit()

                    except (mariadb.ProgrammingError,mariadb.OperationalError) as e:
                        self.logger.info("Sql Programming Error", exc_info=e)

                        self.connection.rollback()
                    finally:
                        cursor.close()
                        del cursor
            except mariadb.Error as e:
                self.logger.warning("MariaDB error", exc_info=e)

        if len(features_geojson_list) == 0:
            return None

        geo_json_feature_collection = geojson.FeatureCollection(features_geojson_list)
        geo_str = geojson.dumps(geo_json_feature_collection, indent=4, ensure_ascii=False)
        with self.obj_lock:
            now_ts = datetime.datetime.now(tz=datetime.timezone.utc)
            if len(self.latest_geojson_jams_fifo_list) < self.geo_json_fifo_size:
                self.latest_geojson_jams_fifo_list.append((geo_str, now_ts))
            else:
                self.latest_geojson_jams_fifo_list.pop(0)
                self.latest_geojson_jams_fifo_list.append((geo_str, now_ts))
            try:
                if self.enable_queue_write:
                    self.traffic_geojson_jams_object_queue.put_nowait(geo_str)
            except queue.Full as e:
                pass
        if self.chk_connection():
            try:
                with self.connection_lock:
                    cursor = self.connection.cursor(buffered=True)
                    self.connection.begin()
                    try:
                        cursor.execute(statement_insert_geojson, (geo_str, traffic_db_id))
                        self.connection.commit()

                    except (mariadb.ProgrammingError,mariadb.OperationalError) as e:
                        self.logger.info("Sql Programming Error", exc_info=e)

                        self.connection.rollback()
                    finally:
                        cursor.close()
            except mariadb.Error as e:
                self.logger.warning("MariaDB error", exc_info=e)
        return geo_str

    def parse_json_data_alerts(self, traffic_db_id: int, object_traffic: Union[object,dict, None])-> Union[str, None]:

        statement_sql_insert = (f"INSERT INTO traffic_db.tblTrafficAlerts ("
                                f"uuid_raw,uuid_int,jam_uuid,"
                                f"country,city,street,"
                                f"municipality_user_report,road_type,type,"
                                f"sub_type,confidence,report_rating,"
                                f"reliability,user_feedback_thup,magvar,"
                                f"description,traffic_timestamp_ms,traffic_timestamp,"
                                f"fk_raw_data,location_lat,location_lon"
                                f") VALUES ("
                                f"%s,%s,%s,"
                                f"%s,%s,%s,"
                                f"%s,%s,%s,"
                                f"%s,%s,%s,"
                                f"%s,%s,%s,"
                                f"%s,%s,%s,"
                                f"%s,%s,%s);")
        statement_insert_geojson = (f"INSERT INTO traffic_db.geojson_archive(geojson_data, fk_raw_data,type) "
                                    f"VALUES (%s,%s,1);")
        if not self.chk_connection():
            return None
        if not isinstance(traffic_db_id, int)  or not isinstance(object_traffic, dict) :
            return None
        alert_list = list()
        if 'alerts' in object_traffic:
            alert_list.extend(object_traffic["alerts"])
        else:
            return None
        err_cnt = 0
        features_geojson_list = list()
        upload_data_list = list()
        traffic_db_id = int(traffic_db_id)
        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)
        for k_idx,i_obj in enumerate(alert_list):
            try:
                #each object is an alert
                country = i_obj.get('country', None)
                city = i_obj.get('city', None)
                street = i_obj.get('street', None)
                confidence = i_obj.get('confidence', None)
                reliability = i_obj.get('reliability',None)
                rating_report = i_obj.get('reportRating',None)
                usr_feedback = i_obj.get('nThumbsUp',None)
                uuid_alert = i_obj.get('uuid', None)
                jam_uuid= i_obj.get('jamUuid', None)
                road_type = i_obj.get('roadType', None)
                magvar = i_obj.get('magvar', None)
                sub_type = i_obj.get('subtype', None)
                location_obj = i_obj.get('location', None)
                if location_obj is not None:
                    location_lat = location_obj.get('y', None)
                    location_lon = location_obj.get('x',None)
                else:
                    location_lat = None
                    location_lon = None
                user_report = i_obj.get('reportByMunicipalityUser', None)
                user_report = str_to_bool(user_report)
                type_alert = i_obj.get('type',None)
                description = i_obj.get('reportDescription',None)
                timestamp_ms = int(i_obj["pubMillis"]) / 1000 if "pubMillis" in i_obj else None
                if timestamp_ms is not None:
                    try:

                        timestamp = datetime.datetime.fromtimestamp(timestamp_ms,tz=datetime.timezone.utc)
                        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError as e:
                        timestamp = None
                        timestamp_str = None

                else:
                    timestamp = None
                    timestamp_str = None
                country = str(country)[:31] if country is not None else None
                city = str(city)[:31] if city is not None else None
                street = str(street)[:249] if street is not None else None
                location_lat = float(location_lat) if location_lat is not None else None
                location_lon = float(location_lon) if location_lon is not None else None
                road_type = int(road_type) if road_type is not None else None
                type_alert = str(type_alert)[:31] if type_alert is not None else None
                sub_type_alert = str(sub_type)[:63] if sub_type is not None else None
                uuid_alert = str(uuid_alert)[:127] if uuid_alert is not None else None
                uuid_int = uuid_to_int(uuid_alert) if uuid_alert is not None else None
                jam_uuid = uuid_to_int(jam_uuid) if is_valid_uuid(jam_uuid) else None
                confidence = int(confidence) if confidence is not None else None
                reliability = int(reliability) if reliability is not None else None
                rating_report = int(rating_report) if rating_report is not None else None
                user_feedback = int(usr_feedback) if usr_feedback is not None else None
                magvar = int(magvar) if magvar is not None else None
                description = str(description)[:1023] if description is not None else None
                entry_up = (uuid_alert, uuid_int, jam_uuid,
                            country, city, street,
                            user_report, road_type, type_alert,
                            sub_type_alert, confidence, rating_report,
                            reliability, user_feedback, magvar,
                            description, timestamp_ms, timestamp_str,
                            traffic_db_id, location_lat, location_lon)
                upload_data_list.append(entry_up)
                properties_feature = {
                    "country":country,
                    "city":city,
                    "street":street,
                    "uuid":uuid_alert,
                    "uuid_int":uuid_int,
                    "description":description,
                    "lat":location_lat,
                    "lon":location_lon,
                    "type":type_alert,
                    "sub_type":sub_type_alert,
                    "time":timestamp_str
                }
                try:
                    point_geojson = geojson.Point((location_lon, location_lat))
                    feature_p = geojson.Feature(id=uuid_alert+"_"+str(k_idx), geometry=point_geojson,
                                                properties=properties_feature or {})
                    features_geojson_list.append(feature_p)
                except (ValueError,AttributeError,TypeError):
                    err_cnt += 1
                    continue

            except (KeyError,ValueError) as err:
                err_cnt += 1
                continue


        alert_feature_collection = geojson.FeatureCollection(features_geojson_list) if (
                len(features_geojson_list) > 0) else None
        geo_json_alerts = geojson.dumps(alert_feature_collection) if alert_feature_collection is not None else None
        if len(upload_data_list) > 0 and self.chk_connection():
            try:

                with self.connection_lock:
                    self.connection.begin()
                    with  self.connection.cursor(buffered=True) as cursor:
                        try:
                            cursor.executemany(statement_sql_insert, upload_data_list)
                            warnings_db = self.connection.show_warnings()
                            if warnings_db is not None:
                                if len(warnings_db) > 0:
                                    for w in warnings_db:
                                        self.logger.info("Sql Warnings: %s category: %s lineno : %s line : %s",
                                                         w.message,w.category,w.lineno,w.line)

                            if geo_json_alerts is not None:
                                cursor.execute(statement_insert_geojson, (geo_json_alerts, traffic_db_id))
                            self.connection.commit()
                        except (mariadb.ProgrammingError,mariadb.OperationalError) as err:
                            self.logger.info("Sql Programming Error", exc_info=err)
                            self.connection.rollback()


            except mariadb.Error as e:
                self.logger.warning("MariaDB error", exc_info=e)
        with self.obj_lock:
            if len(self.latest_geojson_alerts_fifo_list) >= self.geo_json_fifo_size:

                self.latest_geojson_alerts_fifo_list.pop(0)
            self.latest_geojson_alerts_fifo_list.append((geo_json_alerts,current_time_utc))

        return geo_json_alerts





def thread_worker_traffic(traffic_object_instance: TrafficLibClass,
                          stop_event: threading.Event,
                          worker_queue_jobs: queue.Queue,
                          worker_results_dict: dict,
                          worker_results_lock: threading.RLock,
                          pool_time: int = 10):
    """
    Handles a Traffic data processing thread. The thread periodically fetches data from provider,
    processes it, and manages a worker queue for further data handling.

    Parameters:
    traffic_object_instance (TrafficLibClass): Instance of the Traffic library for data interaction.
    stop_event (threading.Event): Event to signal the thread to stop.
    worker_queue_jobs (queue.Queue): Queue of jobs for workers to process.
    worker_results_dict (dict): Dictionary to store results from processed jobs.
    worker_results_lock (threading.RLock): Reentrant lock for synchronizing access to worker results.
    pool_time (int, optional): Time interval for pulling data from provider. Defaults to 10 seconds.

    Returns:
    int: Exit code indicating the result of the thread execution.
    """
    global log_queue
    thread_logger = setup_logger(log_queue, log_level=logging.INFO)
    thread_logger.propagate = False
    thread_logger.info(f"Starting Traffic thread")

    connection_db = traffic_object_instance.connect_to_db()
    thread_logger.info(f"fConnected to DataBase({traffic_object_instance.db_name}) "
                       f"{traffic_object_instance.host_str}:{traffic_object_instance.port} for Traffic data processing")
    latest_pull = None
    enable_queue_processor = True

    max_dequeue = 4096
    last_clean_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    last_mem_snapshot = None
    prev_snapshot = None
    while True:

        if stop_event.is_set():
            traffic_object_instance.empty_queues()
            while not worker_queue_jobs.empty():
                try:
                    worker_queue_jobs.get_nowait()
                    worker_queue_jobs.task_done()
                except queue.Empty:
                    break
            traffic_object_instance.connection.close()
            thread_logger.info("Exiting Tarffic thread")

            return 0
        try:
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            pull_ok = False
            if latest_pull is None:
                pull_ok = True
            elif (now - latest_pull) >= datetime.timedelta(seconds=pool_time):
                pull_ok = True
            # Empty unused queues for now
            if (traffic_object_instance.traffic_geojson_jams_object_queue.full()
                    or traffic_object_instance.traffic_json_object_queue.full()):
                traffic_object_instance.empty_queues()

            if pull_ok:
                fetch_data_tuple: tuple[object, str] = traffic_object_instance.download_json()

                latest_pull = now
                if fetch_data_tuple is None:
                    thread_logger.warning("Fetched Traffic data is None or null")

                else:
                    store_tuple = traffic_object_instance.store_json_raw(fetch_data_tuple, now)
                    if store_tuple is None:
                        thread_logger.warning("Stored Traffic data is None or null")
                    else:
                        db_id, exists_in_db = store_tuple

                        if db_id is None:
                            thread_logger.warning("Null/None db id returned")
                        else:
                            if not exists_in_db:
                                geo_json = traffic_object_instance.parse_json_data_jams(db_id, fetch_data_tuple[0])
                                geo_json_alerts = traffic_object_instance.parse_json_data_alerts(db_id, fetch_data_tuple[0])
                                if geo_json is None:
                                    thread_logger.warning("Null/None geo json returned")
                                del geo_json
                                del geo_json_alerts

                del fetch_data_tuple

            if enable_queue_processor and not worker_queue_jobs.empty():
                # process the incoming and outgoing queue requests
                job_list_descriptor_list: list[tuple[int, uuid.UUID, list, list]] = list()
                k = 0
                while (not worker_queue_jobs.empty()) and k < max_dequeue:
                    try:
                        item_job = worker_queue_jobs.get_nowait()
                        worker_queue_jobs.task_done()
                        job_list_descriptor_list.append(item_job)
                        k += 1
                    except queue.Empty:
                        pass
                job_results = list()
                for job in job_list_descriptor_list:
                    try:
                        job_thread_id = job[0]
                        job_id = job[1]
                        job_list_id_fetch = job[2]
                        job_list_date_fetch = job[3]

                        result_geo: list[tuple[str, datetime,int]] = list()
                        if len(job_list_id_fetch) > 0:
                            try:
                                job_list_id_fetch_parsed = [int(i) for i in job_list_id_fetch if i is not None]
                                result_geo_temp: Union[list[tuple[
                                    str, datetime,int]], None] = traffic_object_instance.get_db_archive_geo_json_multiple(
                                    job_list_id_fetch_parsed)
                                if result_geo_temp is not None:
                                    result_geo.extend(result_geo_temp)
                            except ValueError:
                                pass
                        id_result_list = list()
                        if len(job_list_date_fetch) > 0:
                            for time_tuple in job_list_date_fetch:
                                if time_tuple is None:
                                    continue
                                elif (not isinstance(time_tuple[0], datetime.datetime) and
                                      not isinstance(time_tuple[1], datetime.datetime)):
                                    continue

                                if time_tuple[0] > time_tuple[1]:

                                    result_ids: list[tuple[int, datetime.datetime,int]] = (
                                        traffic_object_instance.get_db_archive_list_by_date(None,
                                                                                            time_tuple[1], time_tuple[0]))
                                else:
                                    result_ids: list[tuple[int, datetime.datetime,int]] = (
                                        traffic_object_instance.get_db_archive_list_by_date(None,
                                                                                            time_tuple[0], time_tuple[1]))
                                id_result_list.append(result_ids)
                        # 0-timestamp 1-thread_id 2-uuid 3 - geo 4 -id
                        job_results.append((now, job_thread_id, job_id, result_geo, id_result_list))
                    except (IndexError, ValueError) as e:
                        thread_logger.critical("Index error or Value error encountered", exc_info=e)
                        break

                # clean dict
                with worker_results_lock:
                    clean_now_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
                    if (clean_now_timestamp - last_clean_timestamp) > datetime.timedelta(seconds=20):
                        last_clean_timestamp = clean_now_timestamp
                        clean_list_keys_threads = list()
                        clean_list_jobs = list()
                        for thread_key in worker_results_dict:
                            entry_count = 0
                            if len(worker_results_dict[thread_key]) == 0:
                                clean_list_keys_threads.append(thread_key)

                            for job_key, job_res in worker_results_dict[thread_key].items():
                                creation_time: datetime.datetime = job_res[0]
                                if (clean_now_timestamp - creation_time) > datetime.timedelta(seconds=60):
                                    clean_list_jobs.append((thread_key, job_key))
                        for i in clean_list_jobs:
                            th_key = i[0]
                            jb_key = i[1]
                            del worker_results_dict[th_key][jb_key]
                        for i in clean_list_keys_threads:
                            del worker_results_dict[i]

                        del clean_list_jobs
                        del clean_list_keys_threads
                        gc.collect()
                    for i in job_results:
                        uuid_str = str(i[2].hex)
                        thread_id = str(i[1])
                        time_created = i[0]
                        if thread_id not in worker_results_dict:
                            worker_results_dict[thread_id] = dict()

                        worker_results_dict[thread_id][uuid_str] = [i[0], i[3], i[4]]

            exec_trace = False
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            if last_mem_snapshot is None:
                last_mem_snapshot = now
                exec_trace = True
            elif abs(now - last_mem_snapshot) >= datetime.timedelta(minutes=30):
                last_mem_snapshot = now
                exec_trace = True
            if exec_trace:
                gc.collect()

                current, peak = tracemalloc.get_traced_memory()

                snapshot_mem = tracemalloc.take_snapshot()

                mem_stats_traceback = snapshot_mem.statistics('traceback')
                thread_logger.info("----Memory-Stat Start(Trace)----")
                # pick the biggest memory block
                graph_objects = list()
                for i, mem_blk in enumerate(mem_stats_traceback[:10]):

                    thread_logger.info("Block %d : %s memory blocks: %.1f MiB", i, mem_blk.count,
                                       mem_blk.size / (1024 * 1024))

                    for line in mem_blk.traceback.format():
                        thread_logger.info("block: %d  line: %s", i, line)
                thread_logger.info("----Memory-Stat Start(lineno)----")
                unique_objects = display_top(snapshot_mem, thread_logger, 'lineno', 20)

                if prev_snapshot is not None:
                    thread_logger.info("----Memory-Stat Start(delta)----")
                    stats_delta = snapshot_mem.compare_to(prev_snapshot, 'lineno')
                    thread_logger.info("Delta Diffs:")
                    for i, stat in enumerate(stats_delta[:20]):
                        thread_logger.info("Diff nr %d : %s", i, stat)
                thread_logger.info(f"Current memory usage is {current / 10 ** 6}MB; Peak was {peak / 10 ** 6}MB")

                prev_snapshot = snapshot_mem
                tracemalloc.reset_peak()







            time.sleep(0.2)

        except RuntimeError as e:
            thread_logger.error("Runtime error in Traffic thread", exc_info=e)
            continue
        except OSError as e:
            thread_logger.error("OSError in Traffic thread", exc_info=e)
            continue
        except mariadb.Error as e:
            thread_logger.error("MariaDB error in Traffic thread", exc_info=e)
            continue
        except (ValueError, KeyError, IndexError,NameError) as e:
            thread_logger.error("Error in Traffic thread", exc_info=e)
            continue
