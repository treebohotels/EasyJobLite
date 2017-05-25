import logging
import socket
from unittest import TestCase

from mock import patch, Mock

logging.basicConfig()

from easyjoblite.orchestrator import Orchestrator
from easyjoblite.consumers import base_rmq_consumer


class TestBaseRMQConsumer(TestCase):
    def setUp(self):
        self.orchestrator = Orchestrator(rabbitmq_url="test.rabbitmq.com:8000")

    @patch('easyjoblite.consumers.base_rmq_consumer.Connection')
    def test_constructor(self, kombu_connection_mock):
        kombu_connection_mock.return_value = True
        base_bms_consumer = base_rmq_consumer.BaseRMQConsumer(self.orchestrator)

        kombu_connection_mock.assert_called_with("test.rabbitmq.com:8000", transport_options={'confirm_publish': True})

    @patch('easyjoblite.consumers.base_rmq_consumer.Connection')
    @patch('easyjoblite.consumers.base_rmq_consumer.Consumer')
    def test_consume(self, kombu_consumer_mock, kombu_connection_mock):
        drain_events = Mock(side_effect=Exception("Test"))
        connection_mock = Mock(drain_events=drain_events)
        consumer_mock = Mock()

        kombu_connection_mock.return_value = connection_mock
        kombu_consumer_mock.return_value = consumer_mock

        base_bms_consumer = base_rmq_consumer.BaseRMQConsumer(self.orchestrator)

        from_queue = Mock()
        from_queue.name = "from_queue_1"
        from_queue.exchange.name = "exchange"

        # Test for blocking scenario

        base_bms_consumer.consume(from_queue=from_queue)

        # check if channel is called
        connection_mock.channel.assert_called()

        # check that consumer is called
        consumer_mock.consume.assert_called()

        # Test for non blocking scenario

        drain_events.side_effect = socket.timeout

        base_bms_consumer.consume(from_queue=from_queue, blocking=False)

        # check if channel is called
        connection_mock.channel.assert_called()

        # check that consumer is called
        consumer_mock.consume.assert_called()

    @patch('easyjoblite.consumers.base_rmq_consumer.Connection')
    def test_process_message(self, kombu_connection_mock):
        base_consumer = base_rmq_consumer.BaseRMQConsumer(self.orchestrator)

        with self.assertRaises(NotImplementedError) as e:
            base_consumer.process_message("Body", "Message")

        self.assertEqual(e.exception.message, "'BaseRMQConsumer' needs to implement process_message(...)")
