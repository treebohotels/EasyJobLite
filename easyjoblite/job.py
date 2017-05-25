# -*- coding: utf-8 -*-

import json

import constants
from easy_api import EasyApi


class EasyJob(object):
    def __init__(self):
        self.tag = "unknown"
        self.job_api = None
        self.no_of_retries = 0
        self.data = None
        self.should_notify_error = False
        self.notification_handler = None
        self.errors = []

    @property
    def api(self):
        return self.job_api.api

    @classmethod
    def create(cls, api, type, remote_call_type=None, data=None, api_request_headers=None,
               content_type=None, should_notify_error=False, tag=None, notification_handler=None):
        job = cls()
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
        job.should_notify_error = should_notify_error
        return job

    @classmethod
    def create_from_dict(cls, dict):
        job = cls()
        if "tag" in dict:
            job.tag = dict["tag"]
        job.data = dict.get("data", None)
        job.should_notify_error = dict.get("should_notify_error", False)
        if "job_api" in dict:
            job.job_api = EasyApi.create_from_dict(dict["job_api"])
        job.no_of_retries = dict.get("no_of_retries", 0)
        if "errors" in dict:
            job.errors = dict["errors"]
        if "notification_handler" in dict:
            job.notification_handler = EasyApi.create_from_dict(dict["notification_handler"])
        return job

    def to_dict(self):
        dict = {
            "job_api": self.job_api.to_dict(),
            "tag": self.tag,
            "data": self.data,
            "should_notify_error": self.should_notify_error,
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
        return self.job_api.execute(data, async_timeout)

    def notify_error(self, data, async_timeout=constants.DEFAULT_ASYNC_TIMEOUT):
        data.update({"errors": self.errors})
        return self.notification_handler.execute(data, async_timeout)

    def increment_retries(self):
        self.no_of_retries += 1
