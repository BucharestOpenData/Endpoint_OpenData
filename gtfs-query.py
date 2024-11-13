import http.server
import urllib.parse
import uuid
import signal
import ssl
from threading import Thread, Event
import socket
import argparse
from typing import Union
from urllib3.exceptions import InsecureRequestWarning
from gtfs_lib import *
import random
import logging, logging.handlers
import time
from traffic_lib import TrafficLibClass, traffic_data_disk_writer
from traffic_lib import TrafficLibClass, traffic_endpoint_url, thread_worker_traffic, log_queue
import gc

random.seed(a=None, version=2)

gc.enable()
log_listener_q: Union[logging.handlers.QueueListener, None] = None
threads_sig_int = threading.Event()
threads_sig_db_init = threading.Event()
resource_data_lock = threading.RLock()
http_server: Union[http.server.ThreadingHTTPServer, None] = None
prov_t_data_queue = queue.Queue(1000)
prov_r_data_queue = queue.Queue(1000)
prov_i_data_queue = queue.Queue(1000)
prov_psg_data_queue = queue.Queue(1000)

prov_r_link_update_thread: Union[None, threading.Thread] = None
prov_t_update_thread: Union[None, threading.Thread] = None
thread_database_sync: Union[None, threading.Thread] = None
thread_prov_int_srv_update: Union[None, threading.Thread] = None
thread_data_aggregate: Union[None, threading.Thread] = None
thread_prov_psg_api_update: Union[None, threading.Thread] = None

worker_traffic_queue_jobs = queue.Queue(4096)
worker_traffic_result_dict = dict()
worker_traffic_lock = threading.RLock()


def update_thread_function_prov_r_link(export_queue_object: queue.Queue,
                                       pool_time: int, prov_r_link_in_mem_obj: Prov_R_LinkInMemObj,
                                       logger_var: logging.Logger) -> int:
    """
    :param export_queue_object: A queue object utilized for exporting items based on their availability in the memory object.
    :param pool_time: The interval in seconds for which the thread should wait before repeating its operations.
    :param prov_r_link_in_mem_obj: An instance of Prov_R_LinkInMemObj responsible for in-memory operations.
    :param logger_var: A logger object used to log various events, messages, and potential exceptions during the thread's execution.
    :return: Integer 0 indicating the thread has terminated.
    """
    global threads_sig_int
    if logger_var is not None:
        logger_var.log(logger_var.level, f"Prov R Update Thread Started with ID:%d",
                       threading.current_thread().native_id,
                       exc_info=True)
    while True:
        if threads_sig_int.is_set():
            logger_var.log(logger_var.level, f"Prov R Update Thread Terminated with ID:%d", )
            return 0
        with resource_data_lock:
            try:
                prov_r_link_in_mem_obj.update()
                if not export_queue_object.full():
                    last_elem = prov_r_link_in_mem_obj.get_in_mem_last()
                    export_queue_object.put(last_elem, block=False)
            except queue.Full:
                print(f"MariaDB Thread Queue Full\n")
                if logger_var is not None:
                    logger_var.debug(msg="Queue Full")
            except (RuntimeError, ConnectionError) as err:
                if logger_var is not None:
                    logger_var.exception(msg="Thread Exception", exc_info=err)

        if threads_sig_int.wait(timeout=pool_time):
            return 0


def database_sync_thread_function(gtfs_db_object: SqlGtfsDatabase, in_queue: queue.Queue, pool_time: int,
                                  vehicle_memory_db_obj: VehicleMemoryDB, logger_var: logging.Logger) -> int:
    global threads_sig_int
    elem = None
    if logger_var is not None:
        logger_var.info(f"MariaDB Thread Started with ID: {threading.current_thread().native_id}")
    thread_start_time = datetime.datetime.now(tz=datetime.timezone.utc)
    enable_table_update = False
    error_flag = 0
    gtfs_db_object.get_vehicle_correlation_data()
    vehicle_memory_db_obj.update_correlation_dict()
    last_update = datetime.datetime.now(tz=datetime.timezone.utc)
    threads_sig_db_init.set()
    while True:
        if threads_sig_int.is_set():
            logger_var.info(f"MariaDB Thread Terminated with ID: {threading.current_thread().native_id}")
            return 0
        try:

            if gtfs_db_object.is_connected() and error_flag == 0:

                array = list()

                try:
                    if (datetime.datetime.now(tz=datetime.timezone.utc) - last_update) > datetime.timedelta(seconds=10):
                        gtfs_db_object.get_vehicle_correlation_data()
                        vehicle_memory_db_obj.update_correlation_dict()
                        last_update = datetime.datetime.now(tz=datetime.timezone.utc)

                    if not enable_table_update:
                        now = datetime.datetime.now(tz=datetime.timezone.utc)
                        td = now - thread_start_time
                        if td.total_seconds() > pool_time * 2:
                            enable_table_update = True
                    while in_queue.qsize() > 0:
                        try:
                            elem = in_queue.get(timeout=0.2)

                            in_queue.task_done()
                            array.append(elem)
                        except queue.Empty:
                            break
                    if len(array) > 0:
                        for i in array:
                            gtfs_db_object.update_vehicle_table(i)
                            if enable_table_update:
                                gtfs_db_object.update_vehicle_position_table(i)
                        if enable_table_update:
                            logger_var.debug(f"Updated DB at:{datetime.datetime.now(tz=datetime.timezone.utc)}")



                except queue.Empty:
                    if logger_var is not None:
                        logger_var.debug("Queue Empty")
            else:
                connection = gtfs_db_object.connect()
                error_flag = 0
                if logger_var is not None:
                    logger_var.info(f"MariaDB Thread Reconnected to the DataBase\n")
                continue

        except ConnectionError as err:
            if logger_var is not None:
                logging.error("MariaDB Thread Connection error occurred", exc_info=err)
            time.sleep(pool_time)
            error_flag = 1
        except RuntimeError_MariaDB as err:
            if logger_var is not None:
                logger_var.exception("MariaDB Thread RuntimeError@MariaDB occurred", exc_info=err)
            time.sleep(pool_time)
            error_flag = 2
        except RuntimeError as err:
            if logger_var is not None:
                logging.exception("MariaDB Thread RuntimeError occurred", exc_info=err)
            time.sleep(pool_time)
            error_flag = 3
        if threads_sig_int.wait(1):
            return 0


def prov_psg_collect_thread_function(query_arg: PsgQuery, pool_interval: int,
                                     export_queue_object: queue.Queue,
                                     logger_var: logging.Logger) -> int:
    global threads_sig_int
    if logger_var is not None:
        logger_var.info(f"PSG API Thread Started with ID: {threading.current_thread().native_id}")
    while True:
        if threads_sig_int.is_set():
            logger_var.info(f"PSG API Thread Terminated with ID: {threading.current_thread().native_id}")
            return 0
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", InsecureRequestWarning)
                data = query_arg.fetch_update()

            if data is not None:
                try:
                    export_queue_object.put(data, block=True, timeout=0.5)
                    logger_var.debug("Updated from PSG API")
                except queue.Full:
                    logger_var.info("PSG Queue Full")

        except RuntimeError as err:
            if logger_var is not None:
                logger_var.exception(f"Runtime Exception occurred in  PSG Query Thread", exc_info=err)
        except ConnectionError as err:
            if logger_var is not None:
                logger_var.warning(f"Connection Exception in PSG API", exc_info=err)
        except RuntimeWarning as warn:
            if logger_var is not None:
                logger_var.info(f"PSG Thread Warning occurred", exc_info=warn)

        except ValueError as err:
            if logger_var is not None:
                logger_var.exception(f"Value Error in PSG Query Thread", exc_info=err)

        except KeyError as err:
            if logger_var is not None:
                logger_var.exception(f"Key Error in PSG Query Thread.", exc_info=err)

        if threads_sig_int.wait(timeout=pool_interval):
            return 0



def prov_t_collect_thread_function(obj_arg: prov_t_api_query, pool_interval: int,
                                   export_queue: queue.Queue,
                                   logger_var: logging.Logger) -> int:
    global threads_sig_int
    update_data = None
    login_status = None
    if logger_var is not None:
        logger_var.info(f"Provider T Update Thread Started with ID: {threading.current_thread().native_id}")
    while True:
        if threads_sig_int.is_set():
            logger_var.info(f"Provider T Update Thread Terminated with ID: {threading.current_thread().native_id}")
            return 0
        try:

            update_data = obj_arg.fetch_update_auto()

        except RuntimeError as err:
            if logger_var is not None:
                logger_var.exception("Runtime Exception occurred in processing thread", exc_info=err)

            update_data = None
            login_status = None
        try:
            if update_data is not None:
                export_queue.put(update_data, block=False)
                if logger_var is not None:
                    logger_var.info(f"Updated from from Provider T at:{datetime.datetime.now(tz=datetime.timezone.utc)}")

        except queue.Full as err:

            logging.info("Queue Full Exception occurred in processing thread.Size:%d",
                         int(export_queue.qsize()),
                         exc_info=True)

        if threads_sig_int.wait(timeout=pool_interval):
            return 0



def provider_internal_thread_update_function(class_obj_in: ProviderInternalQuery, pool_interval: int,
                                             export_queue: queue.Queue,
                                             logger_var: logging.Logger) -> int:
    global threads_sig_int
    if logger_var is not None:
        logger_var.info(f"Provider Internal Update Thread Started with ID: {threading.current_thread().native_id}")
    while True:
        if threads_sig_int.is_set():
            logger_var.info(f"Provider Internal Update Thread Terminated with ID: {threading.current_thread().native_id}")
            return 0
        try:
            raw_data = class_obj_in.do_query_proc()
            export_queue.put(raw_data, block=False)
            if logger_var is not None:
                logger_var.debug(f"Updated from internal provider at:{datetime.datetime.now(tz=datetime.timezone.utc)}")
        except queue.Full as err:
            if logger_var is not None:
                logger_var.info("Queue Full in  processing thread.Size:%d", exc_info=err)
            time.sleep(5)
        except (RuntimeError, ValueError, ConnectionError) as err:
            if logger_var is not None:
                logger_var.exception("Runtime Error/ValueError/ConnectionError occurred in processing thread",
                                     exc_info=err)
            if err.__class__ == ConnectionError:
                time.sleep(10)
                continue
            else:
                return 1
        if threads_sig_int.wait(timeout=pool_interval):
            return 0



def data_aggregate_thread_function(gtfs_class_obj: VehicleMemoryDB,
                                   queue_api_src_r: queue.Queue,
                                   queue_api_src_int: queue.Queue, queue_api_src_t: queue.Queue, queue_api_src_psg: queue.Queue,
                                   db_export_queue: queue.Queue, logger_var: logging.Logger) -> int:
    global threads_sig_int
    if logger_var is not None:
        logger_var.info(f"Data Aggregation Thread Started with ID: {threading.current_thread().native_id}")
    start_up = True
    timestamp_st = datetime.datetime.now(tz=datetime.timezone.utc)
    threads_sig_db_init.wait()
    threads_sig_db_init.clear()
    while True:
        if threads_sig_int.is_set():
            logger_var.info(f"Data Aggregation Thread Terminated with ID: {threading.current_thread().native_id}")
            return 0
        try:
            update = False
            k = 0
            with gtfs_class_obj.obj_lock:

                obj_tmp_array_r = list()
                obj_tmp_array_t = list()
                obj_tmp_array_int = list()
                obj_tmp_array_psg = list()
                while not queue_api_src_r.empty() and not threads_sig_int.is_set():
                    try:
                        obj_s = queue_api_src_r.get(timeout=0.5)
                        obj_tmp_array_r.append(obj_s)
                        queue_api_src_r.task_done()
                        update = True
                        k = k + 1
                    except queue.Empty as err:
                        break
                for i in obj_tmp_array_r:
                    gtfs_class_obj.update_from_s_api(i)

                while not queue_api_src_int.empty() and not threads_sig_int.is_set():
                    try:
                        obj_int_tmp = queue_api_src_int.get(timeout=0.5)
                        obj_tmp_array_int.append(obj_int_tmp)

                        queue_api_src_int.task_done()
                        update = True
                        k = k + 1
                    except queue.Empty as err:
                        break

                for i in obj_tmp_array_int:
                    gtfs_class_obj.update_from_int_api(i)
                while not queue_api_src_t.empty() and not threads_sig_int.is_set():
                    try:
                        obj_th = queue_api_src_t.get(timeout=0.5)
                        obj_tmp_array_t.append(obj_th)
                        queue_api_src_t.task_done()
                        update = True
                        k = k + 1
                    except queue.Empty as err:
                        break
                for i in obj_tmp_array_t:
                    gtfs_class_obj.update_from_th(i)
                while not queue_api_src_psg.empty() and not threads_sig_int.is_set():
                    try:
                        object_tmp = queue_api_src_psg.get(timeout=0.5)
                        obj_tmp_array_psg.append(object_tmp)
                        queue_api_src_psg.task_done()
                        # update = True
                        k = k + 1
                    except queue.Empty as err:
                        break
                for i in obj_tmp_array_psg:
                    gtfs_class_obj.update_from_psg_api(i)

            if update and not start_up:
                export = gtfs_class_obj.gen_output()
                gtfs_class_obj.refresh_serial_cache()
                if len(export) > 0:
                    db_export_queue.put(export)
                    if logger_var is not None:
                        logger_var.debug("Data aggregation finished", extra={'size': len(export), 'data': repr(export)})

            elif start_up and update and k >= 2:
                export = gtfs_class_obj.gen_output()
                if len(export) > 0:
                    db_export_queue.put(export)
                    start_up = False
                    if logger_var is not None:
                        logger_var.info("Startup data Aggregation finished", extra={'size': len(export)})

            elif update and (datetime.datetime.now(tz=datetime.timezone.utc) - timestamp_st) > datetime.timedelta(
                    seconds=25):
                export = gtfs_class_obj.gen_output()
                if len(export) > 0:
                    db_export_queue.put(export)
                    start_up = False
                    if logger_var is not None:
                        logger_var.info("Startup data Aggregation finished using single source single",
                                        extra={'size': len(export)})

            time.sleep(0.5)
        except (queue.Empty, queue.Full) as err:


            if logger_var is not None:
                logger_var.info("Queue Exception occurred in Data Aggregation processing thread", exc_info=err)
        except (RuntimeError, ValueError, KeyError) as err:

            if logger_var is not None:
                logger_var.error("Runtime Exception occurred in Data Aggregation processing thread", exc_info=err)
            return 1


# Multi-Threaded TCP Server
class WebServerHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    """
    Class for handling HTTP requests for a web server.
    Called by a factory function to be  compatible with http server

    """

    # noinspection PyMissingConstructor
    def __init__(self, memorydb_arg: VehicleMemoryDB, traffic_lib: TrafficLibClass, logger_arg: logging.Logger,
                 worker_result_dict: dict,
                 worker_result_lock: threading.RLock,
                 worker_job_queue: queue.Queue,
                 open_api_spec: bytes,
                 *args, **kwargs):
        """
        :param memorydb_arg: Instance of VehicleMemoryDB.
        :param traffic_lib: Instance of TrafficLibClass.
        :param logger_arg: Logger instance for logging information.
        :param worker_result_dict: Dictionary to store worker results.
        :param worker_result_lock: RLock to ensure thread-safe access to worker results.
        :param worker_job_queue: Queue for dispatching worker jobs.
        :param open_api_spec: Byte array representing the OpenAPI specification.
        :param args: Additional positional arguments.
        :param kwargs: Additional keyword arguments.
        """
        self.memory_db = memorydb_arg
        self.logger = logger_arg
        self.traffic_lib = traffic_lib
        self.worker_lock = worker_result_lock
        self.result_dict = worker_result_dict
        self.queue_job_dispatch = worker_job_queue
        self.open_api_spec: bytes = open_api_spec
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        """
        A special method that allows the object to be called as a function.

        :param args: A variable-length argument list.
        :type args: Any
        :param kwargs: A keyword argument dictionary.
        :type kwargs: Any
        :return: None
        :rtype: None
        """
        # super().__init__(*args, **kwargs)
        pass

    # @jit(nopython=False, parallel=False)
    def do_GET(self):
        """
        Handles HTTP GET requests to the server.

        Attributes:
            self: The instance of the HTTP request handler.
            start_time (float): The starting time of the request processing.
            parsed_path (ParseResult): The parsed URL path.
            http_path (str): The path component of the parsed URL.
            query_params (dict): The query parameters from the URL.
            ip_client (str): The client's IP address.
            ip_port (int): The client's port number.
            data (Any): The cached serialized output from the memory database.
            json_obj (str): The JSON string of the data.
            array (list): List of vehicles with passenger information.
            data_geojson_tuple (tuple): Latest geojson serialized data.

        Exceptions:
            socket.error: A generic socket error.
            socket.timeout: A timeout error for the socket.
            ConnectionResetError: The connection was reset by the peer.
            ConnectionError: A generic connection error.
            ConnectionAbortedError: The connection was aborted.

        The function processes different paths from the GET request:
            - "/" or "" : Responds with the cached serialized output.
            - "/psg" : Responds with vehicles that have passenger information.

            - Other paths : Responds with a 400 Invalid Request error.

        In case of an exception, logs the socket exception.

        Logs the execution time for processing the request.
        Note:
            - For "/traffic_jams_query_date" the input should be a url_form encoded get request with start_date and end_date parameters.
            - For
        """
        # response_bytes_object=io.BytesIO()
        start_time = time.time()
        try:
            parsed_path = urllib.parse.urlparse(self.path)
            http_path = parsed_path.path
            query_params = urllib.parse.parse_qs(parsed_path.query)
            ip_client = self.client_address[0]
            ip_port = int(self.client_address[1])
            self.server_version = "GTFS Query Buffer Server v1.0"
            body_raw = None
            body_data = None
            # print("GET client ip address:", self.client_address[0])
            # print("GET path:", http_path)
            try:
                recv_content_length = int(self.headers.get('Content-Length', 0))
            except ValueError:
                recv_content_length = 0
            if recv_content_length > 0:
                body = self.rfile.read(recv_content_length).decode('utf-8')
                body_raw = body
                body_data = urllib.parse.parse_qs(body)
            data = self.memory_db.get_serial_output_cache()

            if len(http_path) < 2 or http_path == "/":
                self.protocol_version = "HTTP/1.1"
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.send_header("Connection", "close")

                json_obj = json.dumps(data, default=json_serial, ensure_ascii=True, indent=2)

                self.send_header("Content-Length", str(len(json_obj.encode('utf-8'))))
                self.end_headers()
                self.wfile.write(bytes(json_obj, 'utf-8'))
                end_time = time.time()


            elif http_path == '/openapi.yaml':
                self.send_response(200)
                self.send_header('Content-Type', 'application/x-yaml')
                self.send_header("Content-Length", str(len(self.open_api_spec)))
                self.end_headers()
                self.wfile.write(self.open_api_spec)
                end_time = time.time()


            elif http_path == '/psg':
                self.protocol_version = "HTTP/1.1"
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.send_header("Connection", "close")


                array = []
                for k in data:
                    if k["vehicle"]["passenger_info"]["on_board"] is not None:
                        array.append(k)

                json_obj = json.dumps(array, default=json_serial, ensure_ascii=True, indent=2)
                del array
                self.send_header("Content-Length", str(len(json_obj.encode('utf-8'))))
                self.end_headers()
                self.wfile.write(bytes(json_obj, 'utf-8'))
                end_time = time.time()
            elif http_path == '/traffic_data_jams':
                self.protocol_version = "HTTP/1.1"
                self.send_response(200)
                self.send_header("Connection", "close")
                self.send_header("Content-type", "application/json")
                data_geojson_tuple = self.traffic_lib.get_latest_geojson_jams_serialized()
                if data_geojson_tuple is not None:
                    data = bytes(data_geojson_tuple[0], 'utf-8')

                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)

                else:

                    resp = bytes("<HTML>No data <BR>", 'utf-8')
                    self.send_header("Content-Length", str(len(resp)))

                    self.end_headers()
                    self.wfile.write(resp)

                end_time = time.time()
            elif http_path == "/traffic_data_alerts":
                self.protocol_version = "HTTP/1.1"
                self.send_response(200)
                self.send_header("Connection", "close")
                self.send_header("Content-type", "application/json")
                data_geojson_tuple = self.traffic_lib.get_latest_geojson_alerts_serialized()
                if data_geojson_tuple is not None:
                    data = bytes(data_geojson_tuple[0], 'utf-8')

                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)

                else:

                    resp = bytes("<HTML>No data <BR>", 'utf-8')
                    self.send_header("Content-Length", str(len(resp)))

                    self.end_headers()
                    self.wfile.write(resp)

                end_time = time.time()
            elif http_path == "/traffic_jams_query":
                self.send_response(200)
                self.protocol_version = "HTTP/1.1"
                self.send_header("Connection", "close")
                self.send_header("Content-type", "application/json")
                job_dispatch_ok = False
                job_out_list = list()
                thread_id: int = threading.get_ident()
                date_time_list = list()
                id_list = list()
                unique_id = str(ip_client) + ";" + start_time.hex() + ";" + str(thread_id)
                job_uuid = uuid.uuid5(uuid.NAMESPACE_URL, unique_id)

                if body_data is not None:
                    if "start_date" in body_data and "end_date" in body_data:
                        start_date_list = list(body_data["start_date"])
                        end_date_list = list(body_data["end_date"])
                        min_k = len(start_date_list) if len(start_date_list) < len(end_date_list) else len(
                            end_date_list)
                        min_k = min_k if min_k <= 10 else 10

                        try:
                            for k in range(min_k):
                                if isinstance(start_date_list[k], str) and isinstance(end_date_list[k], str):
                                    try:
                                        start_d = datetime.datetime.strptime(str(start_date_list[k]),
                                                                             "%Y:%m:%d-%H:%M:%S")
                                        start_d = start_d.replace(tzinfo=datetime.timezone.utc)
                                        stop_d = datetime.datetime.strptime(str(end_date_list[k]), "%Y:%m:%d-%H:%M:%S")
                                        stop_d = stop_d.replace(tzinfo=datetime.timezone.utc)
                                    except ValueError:
                                        continue

                                    date_time_tuple = (start_d, stop_d)
                                    date_time_list.append(date_time_tuple)
                                elif isinstance(start_date_list[k], list) and isinstance(end_date_list[k], list):
                                    min_list_size = len(start_date_list[k]) if len(start_date_list[k]) < len(
                                        end_date_list[k]) else len(end_date_list[k])
                                    for k2 in range(min_list_size):
                                        if isinstance(start_date_list[k][k2], str) and isinstance(end_date_list[k][k2],
                                                                                                  str):
                                            try:
                                                start_d = datetime.datetime.strptime(str(start_date_list[k][k2]),
                                                                                     "%Y:%m:%d-%H:%M:%S")
                                                start_d = start_d.replace(tzinfo=datetime.timezone.utc)
                                                stop_d = datetime.datetime.strptime(str(end_date_list[k][k2]),
                                                                                    "%Y:%m:%d-%H:%M:%S")
                                                stop_d = stop_d.replace(tzinfo=datetime.timezone.utc)
                                                date_time_tuple = (start_d, stop_d)
                                                date_time_list.append(date_time_tuple)
                                            except ValueError:
                                                continue
                                else:
                                    continue
                        except (ValueError, IndexError):
                            pass
                    if "geojson_id" in body_data:
                        geojson_id_list = list(body_data["geojson_id"])
                        geojson_id_list = geojson_id_list[:10]
                        for i in geojson_id_list:
                            elem = None
                            if isinstance(i, str):
                                try:
                                    elem = int(i)
                                    id_list.append(elem)
                                except ValueError:
                                    continue
                            elif isinstance(i, list):
                                for k in i:
                                    if isinstance(k, str):
                                        try:
                                            elem = int(k)
                                            id_list.append(elem)
                                        except ValueError:
                                            continue

                try:
                    if len(date_time_list) > 0 or len(id_list) > 0:
                        self.queue_job_dispatch.put((thread_id, job_uuid, id_list, date_time_list), timeout=0.6)
                        job_dispatch_ok = True
                except queue.Full:
                    job_dispatch_ok = False

                if job_dispatch_ok:
                    time_start_wait_job = time.time()
                    current_time = time.time()
                    finish_req = False
                    while (current_time - time_start_wait_job) < 20:
                        with self.worker_lock:
                            if str(thread_id) in self.result_dict:
                                finish_req = True
                                break
                        time.sleep(0.1)
                    if finish_req:
                        found_result = False
                        result_worker = None
                        current_time = time.time()
                        time_start_wait_job = time.time()
                        while (current_time - time_start_wait_job) < 10:
                            with self.worker_lock:
                                try:
                                    if str(thread_id) not in self.result_dict:
                                        time.sleep(0.1)
                                        continue
                                    thread_dict = self.result_dict[str(thread_id)]

                                    if job_uuid.hex in thread_dict:
                                        result_worker = copy.deepcopy(thread_dict[job_uuid.hex])
                                        found_result = True
                                        del thread_dict[job_uuid.hex]
                                        break

                                except KeyError as err2:
                                    self.logger.warning("Key error in HTTP thread", exc_info=err2)
                                    break

                        if result_worker is not None and found_result:
                            geojson_array: list = result_worker[1].copy()
                            id_list = result_worker[2].copy()
                            obj_ser = {"geojson_array": geojson_array, "id_list": id_list}
                            obj_ser_str = json.dumps(obj_ser, default=json_serial, ensure_ascii=True, indent=2)
                            self.send_header("Content-Length", str(len(obj_ser_str.encode('utf-8'))))
                            self.end_headers()
                            self.wfile.write(bytes(obj_ser_str, 'utf-8'))
                            end_time = time.time()
                        else:
                            resp = bytes(json.dumps(None), 'utf-8')
                            self.send_header("Content-Length", str(len(resp)))

                            self.end_headers()
                            self.wfile.write(resp)
                            end_time = time.time()
                    else:
                        resp = bytes(json.dumps(None), 'utf-8')
                        self.send_header("Content-Length", str(len(resp)))

                        self.end_headers()
                        self.wfile.write(resp)
                        end_time = time.time()
                else:
                    resp = bytes(json.dumps(None), 'utf-8')
                    self.send_header("Content-Length", str(len(resp)))

                    self.end_headers()
                    self.wfile.write(resp)
                    end_time = time.time()

            else:

                self.protocol_version = "HTTP/1.1"
                self.send_response(400)
                self.send_header("Connection", "close")
                resp = bytes("<HTML>Invalid Request<BR>", 'utf-8')
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)
                end_time = time.time()

            del data
        except (socket.error, socket.timeout, BrokenPipeError, ConnectionResetError, ConnectionError,
                ConnectionAbortedError) as err2:
            self.logger.info("Socket Exception occurred in WebServerHTTPRequestHandler", exc_info=err2)

            end_time = time.time()
        except (RuntimeError, KeyError, IndexError, TypeError, ValueError) as err2:
            self.logger.info("Socket Exception occurred in WebServerHTTPRequestHandler", exc_info=err2)
            end_time = time.time()
        finally:
            pass
        # print(f"GET:{end_time - start_time:.2f} seconds")
        self.logger.debug("Exec Time serialize:%d", end_time - start_time)

    def do_POST(self):
        # global variables
        self.protocol_version = "HTTP/1.1"
        self.send_response(400)
        self.end_headers()
        self.wfile.write(bytes("<HTML>Invalid Content-Type Header.Expected JSON<BR>", 'UTF-8'))

        return

    def log_message(self, format, *args):
        if self.logger is not None:
            self.logger.debug(format, *args)
        return


##Signal handler for thread termination
def signal_handler_sigint(num, frame):
    signal_name = signal.Signals(num).name
    print("Received signal:", signal_name)
    logger.debug("Received signal:%s", signal_name)
    threads_sig_int.set()

    prov_psg_data_queue.empty()
    prov_i_data_queue.empty()
    prov_r_data_queue.empty()
    prov_t_data_queue.empty()
    log_queue.empty()
    if log_listener_q is not None:
        while not log_queue.empty():
            try:
                log_queue.get_nowait()
                log_queue.task_done()
            except queue.Empty:
                pass
        log_listener_q.stop()
    http_server.server_close()
    exit(0)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Buffer,DataBase Sync,Query Server Daemon for GTFS endpoint")
    parser.add_argument('-p', '--port', type=int, action='store', default=8080, help='Port number for HTTPS '
                                                                                     'Server', )
    parser.add_argument('-d', '--debug', action='store', default="False", type=str, help='Enable debug mode')
    parser.add_argument('-dp', '--db_port', action='store', default=3306, type=int, help='Port number for '
                                                                                         'MariaDB database '
                                                                                         'connection')
    parser.add_argument('-db', '--database', required=True, type=str, help='MariaDB database')
    parser.add_argument('-ip', '--ip_address', required=True, type=str, default='127.0.0.1',
                        help='IP address of MariaDB server')
    parser.add_argument('-u', '--user', required=True, type=str, default='<admin>', help='User name for MariaDB server')
    parser.add_argument('-pw', '--password', required=True, type=str, default='<PASSWORD>',
                        help='Password for MariaDB server')
    parser.add_argument('-t', '--pool_time', type=int, default=1000, help='Time between requests in milliseconds')
    parser.add_argument('-thu', '--provider_t_user', required=True, type=str, default='<admin>', help='User name for provider')
    parser.add_argument('-thp', '--provider_t_password', required=True, type=str, default='<PASSWORD>',
                        help='Password for provider')
    parser.add_argument('-thpu', '--psg_user', required=True, type=str, default='<admin>',
                        help='User name for PSG API')
    parser.add_argument('-thpp', '--psg_password', required=True, type=str, default='<PASSWORD>',
                        help='Password for  PSG API')
    args = parser.parse_args()
    arg_srv_port = int(args.port)
    arg_db_ip_addr = str(args.ip_address)
    arg_db_port = int(args.db_port)
    arg_db_user = str(args.user)
    arg_db_password = str(args.password)
    arg_pool_time = int(args.pool_time)
    arg_debug_on = (lambda in_arg: (in_arg.upper()) == "TRUE")(args.debug)
    arg_t_usr = str(args.provider_t_user)
    arg_t_password = str(args.provider_t_password)
    arg_psg_user = str(args.psg_user)
    arg_psg_password = str(args.psg_password)
    arg_db_name = str(args.database)
    if arg_db_name is None:
        logger.exception("No valid Database specified")
        exit(1)
    run_path = os.getcwd()

    logger, log_listener = init_main(1024 * 1024 * 24, 1, arg_debug_on, log_queue)
    log_listener_q = log_listener
    logger.info("Execution Path:%s", run_path)
    logger.info("MariaDB Database Name:%s", arg_db_name)
    logger.info("MariaDB IP Address:%s", arg_db_ip_addr)
    logger.info("MariaDB Port:%d", arg_db_port)
    logger.info("Pool/Update Time Interval:%d", arg_pool_time)
    logger.info("Listen Port:%d", arg_srv_port)
    database_gtfs_obj_mariadb = SqlGtfsDatabase(str(arg_db_ip_addr), int(arg_db_port), str(arg_db_user),
                                                str(arg_db_password), arg_db_name)
    vehicle_memory_database = VehicleMemoryDB(24, 10, database_gtfs_obj_mariadb)
    traffic_class_inst = TrafficLibClass(traffic_endpoint_url, str(arg_db_user), str(arg_db_password), "<insert traffic_db name>", str(arg_db_ip_addr),
                                         int(arg_db_port))
    prov_th_obj = prov_t_api_query(username=arg_t_usr, password=arg_t_password)

    s_prov_obj = Prov_R_LinkInMemObj(4)

    internal_prov_obj = ProviderInternalQuery('192.168.111.10', 8092, 10)

    db_data_flux_queue = queue.Queue()
    t_psg_class_obj = PsgQuery(username=arg_psg_user, password=arg_psg_password)


    prov_r_link_update_thread = Thread(target=update_thread_function_prov_r_link, name="prov_r_link_update_thread",
                                       args=(prov_r_data_queue, arg_pool_time, s_prov_obj,
                                             vehicle_memory_database, logger))
    # 2nd Thread for MariaDB connection and sync
    thread_database_sync = Thread(target=database_sync_thread_function, name="database_management_thread_function",
                                  args=(database_gtfs_obj_mariadb, db_data_flux_queue, arg_pool_time,
                                        vehicle_memory_database, logger))

    prov_t_update_thread = Thread(target=prov_t_collect_thread_function, name="prov_t_update_thread",
                                  args=(prov_th_obj, arg_pool_time, prov_t_data_queue,
                                        vehicle_memory_database, logger))

    thread_prov_int_srv_update = Thread(target=provider_internal_thread_update_function, name="thread_prov_int_srv_update",
                                        args=(internal_prov_obj, arg_pool_time, prov_i_data_queue,
                                              vehicle_memory_database, logger))

    thread_data_aggregate = Thread(target=data_aggregate_thread_function, name="data_aggregate_thread",
                                   args=(vehicle_memory_database,
                                         prov_r_data_queue, prov_i_data_queue, prov_t_data_queue,
                                         prov_psg_data_queue,
                                         db_data_flux_queue, logger))


    thread_prov_psg_api_update = Thread(target=prov_psg_collect_thread_function,
                                        name="psg_api_update_thread",
                                        args=(t_psg_class_obj, arg_pool_time, prov_psg_data_queue,
                                              vehicle_memory_database, logger))


    thread_traffic_update = Thread(target=thread_worker_traffic, name="Traffic Data Thread",
                                   args=(traffic_class_inst, threads_sig_int,
                                      worker_traffic_queue_jobs,
                                      worker_traffic_result_dict,
                                      worker_traffic_lock,
                                      60 * 3))
    server_addr = ('', arg_srv_port)

    ssl_context_tls_server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context_tls_server.check_hostname = False
    ssl_context_tls_server.load_cert_chain(certfile='./keys/cert.pem', keyfile='./keys/key.pem')
    signal.signal(signal.SIGINT, signal_handler_sigint)
    try:
        with open("./OpenAPI/spec/base_spec.yaml", 'r') as f:
            bytes_yaml_openapi = f.read().encode(encoding='utf-8')
    except (OSError, UnicodeEncodeError) as e:
        logger.exception("Critical exception reading Open-API YAML", exc_info=e)
        exit(1)

    web_handler = lambda *args, **kwargs: WebServerHTTPRequestHandler(vehicle_memory_database, traffic_class_inst,
                                                                      logger,
                                                                      worker_traffic_result_dict,
                                                                      worker_traffic_lock,
                                                                      worker_traffic_queue_jobs,
                                                                      bytes_yaml_openapi, *args, *kwargs)
    try:
        httpd = http.server.ThreadingHTTPServer(server_addr, web_handler)
        http_server = httpd
    except OSError as err:
        logger.exception("OS Error occurred", exc_info=err)
        exit(1)
    # httpd.socket = sslctx.wrap_socket(httpd.socket, server_side=True)
    httpd.socket = ssl_context_tls_server.wrap_socket(httpd.socket, server_side=True)
    # httpd.server_bind = httpd.server_close = lambda self: None
    try:
        database_gtfs_obj_mariadb.connect()
    except ConnectionError as error:
        logger.error(f"Error connecting to MariaDB server", exc_info=error)
        threads_sig_int.set()
        exit(1)
    thread_database_sync.start()
    thread_data_aggregate.start()
    # prov_t_update_thread.start()
    # sleep(2)
    thread_prov_int_srv_update.start()
    thread_prov_psg_api_update.start()

    thread_traffic_update.start()
    # prov_r_link_update_thread.start()

    httpd.request_queue_size = 1000

    httpd.serve_forever(poll_interval=1)
    thread_database_sync.join()
    thread_data_aggregate.join()
    thread_prov_int_srv_update.join()
    # prov_r_link_update_thread.join()
    prov_t_update_thread.join()
    thread_traffic_update.join()
    httpd.server_close()
    log_listener.stop()
