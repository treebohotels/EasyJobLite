# -*- coding: utf-8 -*-

import json
import random
import string
import time

import constants
from easy_api import EasyApi
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

    @property
    def api(self):
        return self.job_api.api

    def should_notify_error(self):
        return self.notification_handler is not None

    @classmethod
    def generate_id(cls):
        return "-".join(["".join(str(time.time()).split('.')), random.choice(string.digits)])


    @classmethod
    def create(cls, api, type, remote_call_type=None, data=None, api_request_headers=None,
               content_type=None, tag=None, notification_handler=None):
        job = cls()
        job.type = type
        job.id = cls.generate_id()
        job.job_api = EasyApi.create(type, api, api_request_headers, remote_call_type, content_type)
        if notification_handler:
            job.notification_handler = EasyApi.create(type,
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
    def create_from_dict(cls, dict):
        try:
            job = cls()
            if "id" in dict:
                job.id = dict["id"]
            if "type" in dict:
                job.type = dict["type"]
            if "tag" in dict:
                job.tag = dict["tag"]
            job.data = dict.get("data", None)
            if "job_api" in dict:
                job.job_api = EasyApi.create_from_dict(dict["job_api"])
            job.no_of_retries = dict.get("no_of_retries", 0)
            if "errors" in dict:
                job.errors = dict["errors"]
            if "notification_handler" in dict:
                job.notification_handler = EasyApi.create_from_dict(dict["notification_handler"])
        except Exception as e:
            raise UnableToCreateJob(e.message, dict)
        return job

    def to_dict(self):
        dict = {
            "id": self.id,
            "type": self.type,
            "job_api": self.job_api.to_dict(),
            "tag": self.tag,
            "data": self.data,
            "no_of_retries": self.no_of_retries,
            "errors": self.errors
        }
        if self.notification_handler:
            dict["notification_handler"] = self.notification_handler.to_dict()
        else:
            dict["notification_handler"] = None
        return dict

    def add_error(self, error):
        self.errors.append(error)

    def execute(self, data, async_timeout=constants.DEFAULT_ASYNC_TIMEOUT):
        api_dict = dict(job_id= self.id, tag=self.tag, data=data)
        return self.job_api.execute(api_dict, async_timeout)

    def notify_error(self, data, async_timeout=constants.DEFAULT_ASYNC_TIMEOUT):
        error_data = dict(job_id=self.id, tag= self.tag, api=self.job_api.api, data=data, errors=self.errors)
        if self.should_notify_error():
            return self.notification_handler.execute(error_data, async_timeout)

    def increment_retries(self):
        self.no_of_retries += 1
