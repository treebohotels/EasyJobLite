import json
from unittest import TestCase

from easyjoblite import constants
from easyjoblite.consumers.dead_letter_queue_consumer import DeadLetterQueueConsumer
from easyjoblite.orchestrator import Orchestrator
from mock import patch, Mock


class TestRetryQueueConsumer(TestCase):
    def setUp(self):
        self.orchestrator = Orchestrator(rabbitmq_url="test.rabbitmq.com:8000")

    @patch('easyjoblite.consumers.base_rmq_consumer.BaseRMQConsumer.consume')
    def test_consume_from_dead_letter_queue(self, consume):
        from_queue = Mock()
        from_queue.name = "from_queue_1"
        from_queue.exchange.name = "exchange"

        dead_letter_con = DeadLetterQueueConsumer(self.orchestrator)
        dead_letter_con.consume_from_dead_letter_queue(from_queue)

        consume.assert_called()

    @patch('easyjoblite.consumers.dead_letter_queue_consumer.EasyJob')
    def test_process_message(self, easy_job_mock):
        # mock the job to be created in the process_message call
        job_mock = Mock()
        job_mock.tag = "unknown"
        job_mock.no_of_retries = 1
        job_mock.errors = ["test error 1", "test error 2"]
        job_mock.notify_error.return_value.status_code = 200
        easy_job_mock.create_from_dict.return_value = job_mock

        # dummy body
        body = json.dumps({"body": "work body"})

        # message mock
        message = Mock()
        api = "http://test.api.com/test_dest"
        api_request_headers = {"title": "Yippi"}
        headers = {}
        message.headers = headers

        dead_letter_con = DeadLetterQueueConsumer(self.orchestrator)
        dead_letter_con.process_message(body, message)

        job_mock.notify_error.assert_called_with(body, constants.DEFAULT_ASYNC_TIMEOUT)
