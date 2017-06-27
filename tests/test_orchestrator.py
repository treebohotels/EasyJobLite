import json
from unittest import TestCase

from easyjoblite import constants
from easyjoblite.exception import EasyJobServiceNotStarted
from easyjoblite.orchestrator import Orchestrator
from mock import patch, Mock


class TestOrchestrator(TestCase):
    def test_constructor(self):
        # create a basic orchestrator
        orchestrator = Orchestrator(rabbitmq_url="test.rabbitmq.com:8000")

        # test is started should be false
        self.assertEqual(orchestrator._service_inited, False)

        # test that service validation should through an exception
        with self.assertRaises(EasyJobServiceNotStarted) as e:
            orchestrator.validate_init()

        # test when config is set while creating then value is reflected in the config
        orchestrator1 = Orchestrator(rabbitmq_url="test.rabbitmq.com:8000", max_worker_count=34)
        self.assertEqual(orchestrator1.get_config().max_worker_count, 34)

    @patch("easyjoblite.orchestrator.Connection")
    @patch("easyjoblite.orchestrator.Exchange")
    @patch("easyjoblite.orchestrator.Producer")
    def test__setup_entities(self, producer_mock, exchange_mock, connection_mock):
        # create a basic orchestrator
        orchestrator = Orchestrator(rabbitmq_url="test.rabbitmq.com:8000")

        orchestrator.setup_entities()
        exchange_mock.assert_called_with(orchestrator.get_config().get_mq_config(constants.EXCHANGE), type='topic',
                                         durable=True)

        connection_mock.assert_called_with("test.rabbitmq.com:8000", transport_options={'confirm_publish': True})

        producer_mock.assert_called()

    @patch("easyjoblite.orchestrator.Orchestrator.setup_entities")
    @patch("easyjoblite.orchestrator.Orchestrator.create_consumer")
    @patch("easyjoblite.orchestrator.time.sleep")
    def test_start_service(self, sleep_mock, create_cunsumer_mock, _setup_entities_mock):
        # create a basic orchestrator
        orchestrator = Orchestrator(rabbitmq_url="test.rabbitmq.com:8000")

        orchestrator.start_service()
        _setup_entities_mock.assert_called()
        create_cunsumer_mock.assert_called()

        self.assertEqual(create_cunsumer_mock.call_count,
                         constants.DEFAULT_WORKER_COUNT + constants.DEFAULT_RETRY_CONSUMER_COUNT +
                         constants.DEFAULT_DLQ_CONSUMER_COUNT)

    @patch("easyjoblite.orchestrator.time.sleep")
    @patch("easyjoblite.orchestrator.Connection")
    @patch("easyjoblite.orchestrator.Exchange")
    @patch("easyjoblite.orchestrator.Producer")
    @patch("easyjoblite.orchestrator.Orchestrator.create_consumer")
    @patch("easyjoblite.orchestrator.WorkQueueConsumer")
    @patch("easyjoblite.orchestrator.RetryQueueConsumer")
    @patch("easyjoblite.orchestrator.DeadLetterQueueConsumer")
    def test_create_cunsumer(self, dlq_con_mock, rt_con_mock, wrk_con_mock, create_cunsumer_mock, producer_mock,
                             exchange_mock, connection_mock, sleep_mock):
        # create a basic orchestrator
        orchestrator = Orchestrator(rabbitmq_url="test.rabbitmq.com:8000")

        with self.assertRaises(EasyJobServiceNotStarted) as e:
            orchestrator.create_work_cunsumer()

        with self.assertRaises(EasyJobServiceNotStarted) as e:
            orchestrator.create_retry_queue_consumer()

        with self.assertRaises(EasyJobServiceNotStarted) as e:
            orchestrator.create_dead_letter_queue_consumer()

        orchestrator.start_service()

        # test work queue
        orchestrator.create_work_cunsumer()
        wrk_con_mock.assert_called()

        # test retry queue
        orchestrator.create_retry_queue_consumer()
        rt_con_mock.assert_called()

        # test deal letter queue
        orchestrator.create_dead_letter_queue_consumer()
        dlq_con_mock.assert_called()

    @patch("easyjoblite.orchestrator.time.sleep")
    @patch("easyjoblite.orchestrator.Connection")
    @patch("easyjoblite.orchestrator.Exchange")
    @patch("easyjoblite.orchestrator.enqueue")
    @patch("easyjoblite.orchestrator.EasyJob")
    def test_enqueue_job(self, easy_job_mock, enqueue_mock, exchange_mock, connection_mock, sleep_mock):
        # create a basic orchestrator
        orchestrator = Orchestrator(rabbitmq_url="test.rabbitmq.com:8000")
        job_mock = Mock()
        job_mock.tag = "unknown"
        easy_job_mock.create.return_value = job_mock
        body = json.dumps({"body": "work body"})
        api = "http://test.api.com/test_dest"
        api_request_headers = {"title": "Yippi"}

        with self.assertRaises(EasyJobServiceNotStarted) as e:
            orchestrator.enqueue_job(api, constants.API_REMOTE, api_request_headers=api_request_headers, data=body)

        orchestrator.setup_entities()

        orchestrator.enqueue_job(api, constants.API_REMOTE, api_request_headers=api_request_headers, data=body)
        enqueue_mock.assert_called_with(orchestrator._producer, "work", job_mock, body)
