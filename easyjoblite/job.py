# -*- coding: utf-8 -*-

import json
import random
import string
import time

import constants
from easy_api import EasyApi
from easyjoblite.response import EasyResponse
from exception import UnableToCreateJob


class EasyJob(object):
    def __init__(self):
        self.id = "1"
        self.type = None
        self.tag = "unknown"
        self.job_api = None
        self.no_of_retries = 0
        self.data = None
        self.notification_handler = None
        self.errors = []
        self.response = None

    @property
    def api(self):
        return self.job_api.api

    def should_notify_error(self):
        return self.notification_handler is not None

    @classmethod
    def generate_id(cls):
        return "-".join(["".join(str(time.time()).split('.')), random.choice(string.digits)])

    @classmethod
    def create(cls, api, job_type, remote_call_type=None, data=None, api_request_headers=None,
               content_type=None, tag=None, notification_handler=None):
        job = cls()
        job.type = job_type
        job.id = cls.generate_id()
        job.job_api = EasyApi.create(job_type, api, api_request_headers, remote_call_type, content_type)
        if notification_handler:
            job.notification_handler = EasyApi.create(job_type,
                                                      notification_handler,
                                                      api_request_headers,
                                                      "post",
                                                      content_type
                                                      )
        if data:
            job.data = json.dumps(data)
        if tag:
            job.tag = tag
        return job

    @classmethod
    def create_from_dict(cls, dict_data):
        try:
            job = cls()
            if "id" in dict_data:
                job.id = dict_data["id"]
            if "type" in dict_data:
                job.type = dict_data["type"]
            if "tag" in dict_data:
                job.tag = dict_data["tag"]
            job.data = dict_data.get("data", None)
            if "job_api" in dict_data:
                job.job_api = EasyApi.create_from_dict(dict_data["job_api"])
            job.no_of_retries = dict_data.get("no_of_retries", 0)
            if "errors" in dict_data:
                job.errors = dict_data["errors"]
            if "notification_handler" in dict_data:
                job.notification_handler = EasyApi.create_from_dict(dict_data["notification_handler"])
            if "response" in dict_data:
                job.response = EasyResponse(dict_data["response"])
        except Exception as e:
            raise UnableToCreateJob(e.message, dict_data)
        return job

    def to_dict(self):
        dict_data = {
            "id": self.id,
            "type": self.type,
            "tag": self.tag,
            "data": self.data,
            "no_of_retries": self.no_of_retries,
            "errors": self.errors
        }

        if self.response:
            dict_data["response"] = self.response.__dict__
        else:
            dict_data["response"] = None

        if self.job_api:
            dict_data["job_api"] = self.job_api.to_dict()
        else:
            dict_data["job_api"] = None

        if self.notification_handler:
            dict_data["notification_handler"] = self.notification_handler.to_dict()
        else:
            dict_data["notification_handler"] = None
        return dict_data

    def add_error(self, error):
        self.errors.append(error)

    def get_response(self):
        return self.response.__dict__

    def execute(self, data, async_timeout=constants.DEFAULT_ASYNC_TIMEOUT):
        api_dict = dict(job_id=self.id, tag=self.tag, data=data)
        ret_val = self.job_api.execute(api_dict, async_timeout)

        if ret_val.status_code != 200:
            self.errors.append(ret_val.__dict__)
        else:
            self.response = ret_val

        return ret_val

    def notify_error(self, data, async_timeout=constants.DEFAULT_ASYNC_TIMEOUT):
        error_data = dict(job_id=self.id, tag=self.tag, api=self.job_api.api, data=data, errors=self.errors)
        if self.should_notify_error():
            return self.notification_handler.execute(error_data, async_timeout)
        else:
            return EasyResponse(400, "No notification handler defined.")

    def increment_retries(self):
        self.no_of_retries += 1
