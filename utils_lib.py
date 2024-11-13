import binascii
import re
import pathlib
import os
import numpy as np
from typing import Union, Any
import requests
import json
import queue
import threading
import datetime
import argparse
import io
from io import BytesIO
import zipfile
import select
import logging
import hashlib
import base64
import cryptography.exceptions
from cryptography.hazmat.backends import default_backend, openssl
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, dsa, rsa, ec
import logging.handlers
import warnings
from functools import lru_cache, cache, cached_property



logger = logging.getLogger(__name__)
formatter = logging.Formatter('[%(asctime)s] {PID:p%(process)s} {Process:%(processName)s} {Thread_id:%('
                              'thread)d}  {%(threadName)s:%('
                              'funcName)s}  {%(pathname)s:%(lineno)d}  %(levelname)s - %(message)s',
                              '%d-%m-%Y %H:%M:%S')
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
file_handler = logging.handlers.RotatingFileHandler('utils_lib.log', mode="w", maxBytes=10 * 1024 * 1024, backupCount=5)
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)
stream_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)




class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, tuple):
            return json.dumps(list(obj),indent=4)
        return super().default(obj)

class DebugUtil:
    def __init__(self, work_dir: Union[str, pathlib.Path]):
        global logger
        self.work_dir = pathlib.Path(work_dir).resolve()
        self.logger = logger
        try:
            self.work_dir.mkdir(exist_ok=True, parents=True, mode=0o777)
        except (OSError, PermissionError, IOError) as e:
            logger.error("Error occurred in debug lib", exc_info=e)

    def log_json(self, in_object: Any, name: str) -> Union[pathlib.Path, None]:
        file_path = self.work_dir.joinpath(name)
        try:
            
            with open(file_path, 'w', buffering=4096, encoding='utf-8') as f:
                json.dump(in_object, f, indent=4, default=self.json_serial_default,cls=CustomJSONEncoder)
            return file_path
        except (PermissionError, OSError, IOError, TypeError) as e:
            self.logger.error("Error occurred in debug lib", exc_info=e)
            return None

    @staticmethod
    def json_serial_default(obj: Any) -> Union[str,dict,list]:
        new_dict = dict()
        compound = False
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = None
                if isinstance(key, (tuple, list)):
                    new_key = str(key)
                elif isinstance(key, str):
                    if str(key).isnumeric():
                        new_key = int(key)
                else:
                    new_key = key
                new_dict[new_key] = value
            return new_dict
        elif isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        elif isinstance(obj, tuple):
            return json.dumps(list(obj),indent=4)
        else:

            try:
                str_val = str(obj)
                return str_val
            except (json.JSONDecodeError, ValueError, binascii.Error) as e:
                raise TypeError(f"Type {type(obj)} not serializable") from e
