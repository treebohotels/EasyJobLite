import sys
from unittest import TestCase

import os
import test_class
from easyjoblite import constants
from easyjoblite.easy_api import EasyApi
from test_class import TestClass
from test_class import dummy_function_external


class TestEasyApi(TestCase):
    def test_create(self):
        sys.path += os.path.dirname(test_class.__file__)

        # create remote api
        job = EasyApi.create(constants.API_REMOTE, "test_api", api_request_headers={"test": "value"},
                             remote_call_type="post", content_type="application/test")

        self.assertEqual(job.type, constants.API_REMOTE)
        self.assertEqual(job.api, "test_api")
        self.assertEqual(job.api_request_headers, {"test": "value"})
        self.assertEqual(job.remote_call_type, "post")
        self.assertEqual(job.content_type, "application/test")
        self.assertEqual(job._instance, None)
        self.assertEqual(job._func_name, "test_api")

        # test local call
        # method
        job = EasyApi.create(constants.API_LOCAL, dummy_function_external)

        self.assertEqual(job.type, constants.API_LOCAL)
        self.assertEqual(job.api, "tests.test_class.dummy_function_external")
        self.assertEqual(job.api_request_headers, {})
        self.assertEqual(job.remote_call_type, "post")
        self.assertEqual(job.content_type, "application/json")
        self.assertEqual(job._instance, None)
        self.assertEqual(job._func_name, "tests.test_class.dummy_function_external")

        # class method
        test_cls = TestClass()
        job = EasyApi.create(constants.API_LOCAL, test_cls.dummy_function_in_class)
        self.assertEqual(job.api, "dummy_function_in_class")
        self.assertEqual(job._instance, test_cls)
        self.assertEqual(job._func_name, "dummy_function_in_class")

        # callable class
        job = EasyApi.create(constants.API_LOCAL, test_cls)
        self.assertEqual(job.api, "__call__")
        self.assertEqual(job._instance, test_cls)
        self.assertEqual(job._func_name, "__call__")
