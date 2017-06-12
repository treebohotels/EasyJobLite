# -*- coding: utf-8 -*-

import importlib
import inspect
import logging
import pickle
import traceback

import constants
import exception
import requests
from response import EasyResponse
from utils import as_text, is_string_type

logging.basicConfig()


class EasyApi(object):
    def __init__(self):
        self.type = constants.API_REMOTE
        self._instance = None
        self._func_name = None
        self.api_request_headers = {}
        self.remote_call_type = "post"
        self.content_type = "application/json"
        self._data = None

    @classmethod
    def create(cls, type, api, api_request_headers={}, remote_call_type=None, content_type=None):
        if type not in constants.job_call_types:
            raise TypeError("Invalid remote_call_type: {}".format(type))
        easy_api = cls()
        easy_api.type = type
        easy_api._instance = None
        easy_api._func_name = None
        easy_api.api_request_headers = api_request_headers
        if remote_call_type:
            easy_api.remote_call_type = remote_call_type
        if content_type:
            easy_api.content_type = content_type

        if easy_api.type == constants.API_LOCAL:
            if inspect.ismethod(api):
                easy_api._instance = api.__self__
                easy_api._func_name = api.__name__
            elif inspect.isfunction(api) or inspect.isbuiltin(api):
                easy_api._func_name = '{0}.{1}'.format(api.__module__, api.__name__)
            elif is_string_type(api):
                easy_api._func_name = as_text(api)
            elif not inspect.isclass(api) and hasattr(api, '__call__'):  # a callable class instance
                easy_api._instance = api
                easy_api._func_name = '__call__'
            else:
                raise TypeError('Expected a callable or a string for local job type, but got: {0}'.format(api))
        else:
            easy_api._func_name = api
        job_tuple = easy_api._func_name, easy_api._instance
        easy_api._data = pickle.dumps(job_tuple)
        return easy_api

    def load_data(self):
        return pickle.loads(self._data)

    @classmethod
    def create_from_dict(cls, dict_data):
        if not dict_data:
            return None

        easy_api = cls()
        if "type" in dict_data:
            easy_api.type = dict_data["type"]
        if "data" in dict_data:
            easy_api._data = dict_data["data"]
            easy_api._func_name, easy_api._instance = easy_api.load_data()
        if "api_request_headers" in dict_data:
            easy_api.api_request_headers = dict_data["api_request_headers"]
        if "remote_call_type" in dict_data:
            easy_api.remote_call_type = dict_data["remote_call_type"]
        if "content_type" in dict_data:
            easy_api.content_type = dict_data["content_type"]

        return easy_api

    def to_dict(self):
        dict_data = {
            "type": self.type,
            "instance": pickle.dumps(self._instance, protocol=0),
            "data": self._data,
            "api_request_headers": self.api_request_headers,
            "remote_call_type": self.remote_call_type,
            "content_type": self.content_type
        }
        return dict_data

    @property
    def api(self):
        return self._func_name

    @property
    def func(self):
        func_name = self._func_name
        if func_name is None:
            return None

        if self._instance:
            return getattr(self._instance, func_name)

        module_name, attribute = func_name.rsplit('.', 1)
        module = importlib.import_module(module_name)
        return getattr(module, attribute)

    def _call_remote(self, api, call_type, data, async_timeout):
        request_call_type = constants.remote_call_type.get(call_type)
        request_headers = self.api_request_headers
        request_headers['Content-type'] = self.content_type
        if call_type == 'post':
            response = request_call_type(api, data=data, timeout=async_timeout,
                                         headers=request_headers)
        else:
            response = request_call_type(api, timeout=async_timeout,
                                         headers=request_headers)
        return response

    def _call_local(self, func_call, data):
        return func_call(data)

    def execute(self, data, async_timeout=constants.DEFAULT_ASYNC_TIMEOUT):
        """
        execute the api call
        :param data: data to be sent while executing
        :param async_timeout: time out for the api (honoured only in case of remote)
        :return: 
        """
        logger = logging.getLogger(self.__class__.__name__)
        ret_val = EasyResponse(200, "")
        try:
            if self.type == constants.API_REMOTE:
                try:
                    call_type = self.remote_call_type
                    response = self._call_remote(self.api, call_type, data, async_timeout)

                except requests.exceptions.Timeout as e:
                    raise exception.ApiTimeoutException(self.api, async_timeout)

                ret_val.status_code = response.status_code
                ret_val.message = response.text

            elif self.type == constants.API_LOCAL:
                response = self._call_local(self.func, data)
                if response and not is_string_type(response):
                    if hasattr(response, 'status_code'):
                        ret_val.status_code = response.status_code
                    if hasattr(response, 'message'):
                        ret_val.message = response.message
                    if hasattr(response, 'data'):
                        ret_val.data = response.data
                    else:
                        ret_val.data = response
                logger.info("Calling api {api} with {data}".format(api=self.api, data=data))
        except Exception as e:
            traceback.print_exc()
            error_message = "Error running API '{api}': {err}".format(api=self.api, err=e.__class__.__name__)
            logger.error(error_message)
            ret_val.status_code = 400
            ret_val.message = error_message
            ret_val.data = e.__dict__
        return ret_val
