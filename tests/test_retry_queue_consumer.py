import json
import socket
from unittest import TestCase

from mock import patch, Mock

from easyjoblite import constants
from easyjoblite.consumers.retry_queue_consumer import RetryQueueConsumer
from easyjoblite.job import EasyJob
from easyjoblite.orchestrator import Orchestrator


class TestRetryQueueConsumer(TestCase):
    def setUp(self):
        self.orchestrator = Orchestrator(rabbitmq_url="test.rabbitmq.com:8000")

    @patch('easyjoblite.consumers.base_rmq_consumer.Connection')
    @patch('easyjoblite.consumers.retry_queue_consumer.Consumer')
    def test__shovel_to_buffer(self, kombu_consumer_mock, kombu_connection_mock):
        drain_events = Mock(side_effect=Exception("Test"))
        drain_events.side_effect = socket.timeout
        connection_mock = Mock(drain_events=drain_events)
        consumer_mock = Mock()

        kombu_connection_mock.return_value = connection_mock
        kombu_consumer_mock.return_value = consumer_mock

        retry_consumer = RetryQueueConsumer(self.orchestrator)

        from_queue = Mock()
        from_queue.name = "from_queue_1"
        from_queue.exchange.name = "exchange"

        retry_consumer._shovel_to_buffer(from_queue=from_queue)

        # check if channel is called
        connection_mock.channel.assert_called()

        # check that consumer is called
        consumer_mock.consume.assert_called()

        # check at the end cancel is called
        consumer_mock.cancel.assert_called()

    @patch('easyjoblite.consumers.base_rmq_consumer.BaseRMQConsumer.produce_to_queue')
    @patch('easyjoblite.consumers.retry_queue_consumer.EasyJob')
    def test__shoveller(self, easy_job_mock, produce_to_queue_mock):
        job_mock = Mock()
        job_mock.tag = "unknown"
        easy_job_mock.create_from_dict.return_value = job_mock
        retry_consumer = RetryQueueConsumer(self.orchestrator)
        body = json.dumps({"body": "work body"})
        message = Mock()
        api = "http://test.api.com/test_dest"
        api_request_headers = {"title": "Yippi"}
        job = EasyJob.create(api, constants.API_REMOTE, api_request_headers=api_request_headers,
                             should_notify_error=True)
        headers = {}
        headers.update(job.to_dict())
        message.headers = headers

        retry_consumer._shoveller(body, message)

        produce_to_queue_mock.assert_called_with(constants.BUFFER_QUEUE, body, job_mock)

    @patch('easyjoblite.consumers.base_rmq_consumer.BaseRMQConsumer.produce_to_queue')
    @patch('easyjoblite.consumers.retry_queue_consumer.EasyJob')
    def test_process_message(self, easy_job_mock, produce_to_queue_mock):
        # mock the job to be created in the process_message call
        job_mock = Mock()
        job_mock.tag = "unknown"
        job_mock.no_of_retries = 1
        easy_job_mock.create_from_dict.return_value = job_mock

        retry_consumer = RetryQueueConsumer(self.orchestrator)
        body = json.dumps({"body": "work body"})
        message = Mock()
        api = "http://test.api.com/test_dest"
        api_request_headers = {"title": "Yippi"}
        job = EasyJob.create(api, constants.API_REMOTE, api_request_headers=api_request_headers,
                             should_notify_error=True)
        headers = {}
        headers.update(job.to_dict())
        message.headers = headers

        # when no of retires is less than max then add back to work queue
        retry_consumer.process_message(body, message)
        produce_to_queue_mock.assert_called_with(constants.WORK_QUEUE, body, job_mock)
        message.ack.assert_called()
        message.reset_mock()

        # test exception
        produce_to_queue_mock.side_effect = Exception()
        retry_consumer.process_message(body, message)
        message.assert_not_called()

        message.reset_mock()

        # when no of retries is more than max retries then add to dead letter queue
        produce_to_queue_mock.side_effect = None
        job_mock.no_of_retries = constants.DEFAULT_MAX_JOB_RETRIES + 1
        retry_consumer.process_message(body, message)
        produce_to_queue_mock.assert_called_with(constants.DEAD_LETTER_QUEUE, body, job_mock)
        message.ack.assert_called()

        # test exception
        message.reset_mock()
        produce_to_queue_mock.side_effect = Exception()
        retry_consumer.process_message(body, message)
        message.assert_not_called()

    @patch('easyjoblite.consumers.retry_queue_consumer.RetryQueueConsumer._shovel_to_buffer')
    @patch('easyjoblite.consumers.retry_queue_consumer.RetryQueueConsumer.consume')
    @patch('easyjoblite.consumers.retry_queue_consumer.time.sleep')
    def test_consume_from_retry_queue(self, sleep_mock, consume_mock, shovel_mock):
        from_queue = Mock()
        from_queue.name = "from_queue"
        from_queue.exchange.name = "exchange"

        buffer_queue = Mock()
        buffer_queue.name = "buffer_queue"
        buffer_queue.exchange.name = "exchange"

        retry_consumer = RetryQueueConsumer(self.orchestrator)

        retry_consumer.consume_from_retry_queue(from_queue, buffer_queue, run_loop=False)

        # check shovel is called
        shovel_mock.assert_called_with(from_queue)

        # then consume is called
        consume_mock.assert_called_with(buffer_queue, blocking=False)

        # then sleep is called
        sleep_mock.assert_called_with(constants.DEFAULT_ERROR_Q_CON_SLEEP_DURATION * 60)
