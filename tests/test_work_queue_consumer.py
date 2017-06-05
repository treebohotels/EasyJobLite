import json
import logging
import sys
from unittest import TestCase

import os
import test_class
from easyjoblite import constants
from easyjoblite.consumers import work_queue_consumer
from easyjoblite.job import EasyJob
from easyjoblite.orchestrator import Orchestrator
from mock import patch, Mock

logging.basicConfig()


class TestWorkQueueConsumer(TestCase):
    def setUp(self):
        self.orchestrator = Orchestrator(rabbitmq_url="test.rabbitmq.com:8000")

    @patch('easyjoblite.consumers.base_rmq_consumer.BaseRMQConsumer.consume')
    def test_consume_from_work_queue(self, consume):
        from_queue = Mock()
        from_queue.name = "from_queue_1"
        from_queue.exchange.name = "exchange"

        work_queue_con = work_queue_consumer.WorkQueueConsumer(self.orchestrator)
        work_queue_con.consume_from_work_queue(from_queue)

        consume.assert_called()

    @patch('easyjoblite.consumers.base_rmq_consumer.BaseRMQConsumer.produce_to_queue')
    @patch('easyjoblite.consumers.work_queue_consumer.EasyJob')
    def test__push_message_to_error_queue(self, easy_job_mock, produce_to_queue):
        # mock the job to be created in the process_message call
        job_mock = Mock()
        job_mock.tag = "unknown"
        job_mock.no_of_retries = 1
        easy_job_mock.create_from_dict.return_value = job_mock

        body = {"body": "work body"}
        message = Mock()
        headers = {"num_retries": 0, "type": "test"}
        message.headers = headers

        work_queue_con = work_queue_consumer.WorkQueueConsumer(self.orchestrator)
        work_queue_con._push_message_to_error_queue(body, message, "some error")

        # check is produce to queue is called
        produce_to_queue.assert_called_with(constants.RETRY_QUEUE, body, job_mock)
        job_mock.increment_retries.assert_called()

        # check is produce when tried for 2 more times sends it to dead letter queue
        job_mock.no_of_retries = 3
        work_queue_con._push_message_to_error_queue(body, message, "some error")
        produce_to_queue.assert_called_with(constants.DEAD_LETTER_QUEUE, body, job_mock)

        # when exception is the thrown
        headers = {"num_retries": 0, "type": "test"}
        message.headers = headers
        produce_to_queue.side_effect = Exception()

        work_queue_con._push_message_to_error_queue(body, message, "some error")

    @patch('easyjoblite.consumers.base_rmq_consumer.BaseRMQConsumer.produce_to_queue')
    @patch('easyjoblite.consumers.work_queue_consumer.EasyJob')
    def test__push_msg_to_dlq(self, easy_job_mock, produce_to_queue):
        job_mock = Mock()
        job_mock.tag = "unknown"
        job_mock.no_of_retries = 1
        easy_job_mock.create_from_dict.return_value = job_mock

        body = {"body": "work body"}
        message = Mock()
        api_request_headers = {"title": "Yippi"}
        job = EasyJob.create("test_api", constants.API_REMOTE, api_request_headers=api_request_headers)
        headers = {}
        headers.update(job.to_dict())
        message.headers = headers

        work_queue_con = work_queue_consumer.WorkQueueConsumer(self.orchestrator)
        work_queue_con._push_msg_to_dlq(body, message, "some error")

        job_mock.add_error.assert_called_with("some error")
        produce_to_queue.assert_called_with(constants.DEAD_LETTER_QUEUE, body, job_mock)

    @patch("easyjoblite.consumers.work_queue_consumer.WorkQueueConsumer._push_message_to_error_queue")
    @patch("easyjoblite.consumers.work_queue_consumer.WorkQueueConsumer._push_msg_to_dlq")
    @patch("easyjoblite.constants.remote_call_type")
    def test_process_message(self, remote_call_type_mock, push_dlq_mock, push_retry_mock):
        remote_call_mock = Mock()
        post = Mock()
        response = Mock()
        response.status_code = 200
        post.return_value = response
        remote_call_type_mock.get.return_value = post

        # Test remote job flow

        body = json.dumps({"body": "work body"})
        message = Mock()
        api = "http://test.api.com/test_dest"
        api_request_headers = {"title": "Yippi"}
        job = EasyJob.create(api, constants.API_REMOTE, api_request_headers=api_request_headers)
        headers = {}
        headers.update(job.to_dict())
        message.headers = headers
        work_queue_con = work_queue_consumer.WorkQueueConsumer(self.orchestrator)
        work_queue_con.process_message(body, message)

        data_body = {'tag': 'unknown', 'data': body, 'job_id': job.id}

        # when return in 200
        post.assert_called_with(api, data=data_body, timeout=constants.DEFAULT_ASYNC_TIMEOUT,
                                headers=api_request_headers)

        # when the status code is 410 (in the error list to be reported
        # then the job will be added to be dlq
        response.status_code = 410
        response.text = "big error"
        work_queue_con.process_message(body, message)
        push_dlq_mock.assert_called_with(body=body, message=message, err_msg=response.text)

        # when the status code is 5XX then add to the error queue
        response.status_code = 520
        response.text = "big error"
        work_queue_con.process_message(body, message)
        push_retry_mock.assert_called_with(body, message, response.text)

        # test local flow

        sys.path += os.path.dirname(test_class.__file__)

        # test with module function
        api = test_class.dummy_function_external
        job = EasyJob.create(api, constants.API_LOCAL)

        headers = {}
        headers.update(job.to_dict())
        message.headers = headers
        test_class.TestClass.module_function_called = False
        work_queue_con.process_message(body, message)
        self.assertEqual(test_class.TestClass.module_function_called, True)

        # test with string function
        api = "tests.test_class.dummy_function_external"
        job = EasyJob.create(api, constants.API_LOCAL)

        headers = {}
        headers.update(job.to_dict())
        message.headers = headers
        test_class.TestClass.module_function_called = False
        work_queue_con.process_message(body, message)
        self.assertEqual(test_class.TestClass.module_function_called, True)

        # test with instance function
        test_cls = test_class.TestClass()
        api = test_cls.dummy_function_in_class
        job = EasyJob.create(api, constants.API_LOCAL)

        headers = {}
        headers.update(job.to_dict())
        message.headers = headers
        test_class.TestClass.class_function_called = False
        work_queue_con.process_message(body, message)
        self.assertEqual(test_class.TestClass.class_function_called, True)

        # test with instance class
        tst_class = test_class.TestClass()
        job = EasyJob.create(tst_class, constants.API_LOCAL)

        headers = {}
        headers.update(job.to_dict())
        message.headers = headers
        test_class.TestClass.class_instance_called = False
        work_queue_con.process_message(body, message)
        self.assertEqual(test_class.TestClass.class_instance_called, True)
