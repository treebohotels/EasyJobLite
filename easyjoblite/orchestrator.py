# -*- coding: utf-8 -*-

import subprocess
import time
from exceptions import KeyError
from multiprocessing import Process

import constants
import os
from configuration import Configuration
from easyjoblite import state
from easyjoblite.consumers.dead_letter_queue_consumer import DeadLetterQueueConsumer
from easyjoblite.consumers.retry_queue_consumer import RetryQueueConsumer
from easyjoblite.consumers.work_queue_consumer import WorkQueueConsumer
from easyjoblite.utils import update_import_paths
from exception import EasyJobServiceNotStarted
from job import EasyJob
from kombu import Connection
from kombu import Exchange
from kombu import Producer
from kombu import Queue
from kombu.entity import PERSISTENT_DELIVERY_MODE


class Orchestrator(object):
    def __init__(self, **kwargs):
        self.consumer_creater_map = {
            constants.WORK_QUEUE: self.create_work_cunsumer,
            constants.RETRY_QUEUE: self.create_retry_queue_consumer,
            constants.DEAD_LETTER_QUEUE: self.create_dead_letter_queue_consumer
        }

        self._config = Configuration(rabbitmq_url=kwargs['rabbitmq_url'])

        self.set_config(**kwargs)
        self._service_started = False

    def validate_init(self):
        if not self._service_started:
            raise EasyJobServiceNotStarted()

    def get_connection(self):
        self.validate_init()
        return self._conn

    def get_config(self):
        return self._config

    def update_consumer_pid(self, type, pid):
        service_state = state.ServiceState(self.get_config().pid_file_path)

        service_state.add_worker_pid(type, pid)

    def set_config(self, **kwargs):
        """
            set config values
        :param kwargs: contains the dict with all key values
        :return: 
        """
        if 'async_timeout' in kwargs:
            self._config.async_timeout = kwargs['async_timeout']

        if 'max_retries' in kwargs:
            self._config.max_retries = kwargs['max_retries']

        if 'eqc_sleep_duration' in kwargs:
            self._config.eqc_sleep_duration = kwargs['eqc_sleep_duration']

        if 'import_paths' in kwargs:
            self._config.import_paths = kwargs['import_paths']
            update_import_paths(self._config.import_paths)

        if 'max_worker_count' in kwargs:
            self._config.max_worker_count = kwargs['max_worker_count']

        if 'default_worker_count' in kwargs:
            self._config.default_worker_count = kwargs['default_worker_count']

        if 'default_retry_consumer_count' in kwargs:
            self._config.default_retry_consumer_count = kwargs['default_retry_consumer_count']

        if 'default_dl_consumer_count' in kwargs:
            self._config.default_dl_consumer_count = kwargs['default_dl_consumer_count']

    def start_service(self, is_detached=False):
        """
        starts the service
        :return: 
        """
        # Setup the entities
        self.setup_entities()

        # create all consumers
        self.create_all_consumers(is_detached)

    def create_all_consumers(self, is_detached=False):
        """
        creates all the consumes needed by the service
        :param is_detached: 
        :return: 
        """
        # create the workers
        for i in range(self._config.default_worker_count):
            self.create_consumer(constants.WORK_QUEUE, is_detached)
            time.sleep(2)

        # create the retry queue consumers
        for i in range(self._config.default_retry_consumer_count):
            self.create_consumer(constants.RETRY_QUEUE, is_detached)
            time.sleep(2)

        # create the dead letter consumers
        for i in range(self._config.default_dl_consumer_count):
            self.create_consumer(constants.DEAD_LETTER_QUEUE, is_detached)
            time.sleep(2)

    def setup_entities(self):
        """
        declare all required entities
        no advanced error handling yet (like error on declaration with altered properties etc)
        """
        # return if already inited
        if self._service_started:
            return

        # setup exchange
        self._booking_exchange = Exchange("booking-exchange",
                                          type='topic',
                                          durable=True)

        # setup durable queues
        self.work_queue = Queue("work-queue",
                                exchange=self._booking_exchange,
                                routing_key=constants.WORK_QUEUE + ".#",
                                durable=True)

        self.retry_queue = Queue("retry-queue",
                                 exchange=self._booking_exchange,
                                 routing_key=constants.RETRY_QUEUE + ".#",
                                 durable=True)

        self.dlq_queue = Queue("dead-letter-queue",
                               exchange=self._booking_exchange,
                               routing_key=constants.DEAD_LETTER_QUEUE + ".#",
                               durable=True)

        # a buffer queue is needed by error-queue-consumer to temp-buffer msgs for processing
        # this is to handle retry loop which may cause between retry-queue and work-queue.
        # todo: Need to implement an alternive as this has a copy overhead
        #  which can be significant when the error queue is large
        self.buffer_queue = Queue(name='buffer-queue',
                                  exchange=self._booking_exchange,
                                  routing_key='buffer.#',
                                  durable=True)

        # todo: do we need to make confirm_publish configurable?
        self._conn = Connection(self.get_config().rabbitmq_url,
                                transport_options={'confirm_publish': True})

        # declare all the exchanges and queues needed (declare, not overwrite existing)
        for entity in [self._booking_exchange, self.work_queue, self.retry_queue,
                       self.dlq_queue, self.buffer_queue]:
            entity.maybe_bind(self._conn)
            entity.declare()

        # setup producer to push to error and dlqs
        self._producer = Producer(channel=self._conn.channel(),
                                  exchange=self._booking_exchange)

        self._service_started = True

    def create_consumer(self, type, is_detached=False):
        if is_detached:
            self.create_consumer_detached(type)
        else:
            self.fork_consumer(type)

    def fork_consumer(self, type):
        p = Process(target=self.start_consumer, args=(type,))
        p.start()
        self.update_consumer_pid(type, p.pid)

    def create_consumer_detached(self, type):

        command_str = constants.BASE_COMMAND

        # Add command
        command_str += " start "

        # Add type
        command_str += str(type)

        # Add rabbitmq url
        command_str += " --url " + str(self.get_config().rabbitmq_url)

        # Add asyn timeout
        command_str += " --asyc_timeout " + str(self.get_config().async_timeout)

        # Add retry count
        command_str += " --max_retries " + str(self.get_config().max_retries)

        # Add retry queue sleep time
        command_str += " --eqc_sleep_duration " + str(self.get_config().eqc_sleep_duration)

        # Add import paths
        command_str += " --import_paths " + str(self.get_config().import_paths)

        # append log to log path
        command_str += " >" + self.get_config().workers_log_file_path

        # run in background
        command_str += "&"

        subprocess.Popen(command_str, shell=True)

    def stop_server(self, type=constants.STOP_TYPE_ALL):
        """
        Stop the server and kill all the workers
        :param type: the type of command
        :return: 
        """
        command_str = constants.BASE_COMMAND

        # Add stop command
        command_str += " stop "

        # Add type of command
        command_str += type

        # run in background
        command_str += " &"

        os.system(command_str)

    def start_consumer(self, type):
        self.setup_entities()

        if type not in self.consumer_creater_map:
            raise KeyError("Invalid consumer type")

        self.consumer_creater_map[type]()

    def create_work_cunsumer(self):
        self.validate_init()
        worker = WorkQueueConsumer(self)
        worker.consume_from_work_queue(self.work_queue)

    def create_retry_queue_consumer(self):
        self.validate_init()
        retry_consumer = RetryQueueConsumer(self)
        retry_consumer.consume_from_retry_queue(self.retry_queue, self.buffer_queue)

    def create_dead_letter_queue_consumer(self):
        self.validate_init()
        dead_letter_consumer = DeadLetterQueueConsumer(self)
        dead_letter_consumer.consume_from_dead_letter_queue(self.dlq_queue)

    def enqueue_job(self, api, type, tag=None, remote_call_type=None, data=None, api_request_headers=None,
                    content_type=None, should_notify_error=False, notification_handler=None):
        self.validate_init()

        # create the job
        job = EasyJob.create(api, type, tag=tag, remote_call_type=remote_call_type, data=data,
                             api_request_headers=api_request_headers,
                             content_type=content_type, should_notify_error=should_notify_error,
                             notification_handler=notification_handler)

        # enqueue
        self.enqueue(constants.WORK_QUEUE, job, data)

        return job.id

    def enqueue(self, queue_type, job, body):
        self.validate_init()
        routing_key = "{type}.{tag}".format(type=queue_type, tag=job.tag)
        headers = job.to_dict()
        self._producer.publish(body=body,
                               headers=headers,
                               routing_key=routing_key,
                               delivery_mode=PERSISTENT_DELIVERY_MODE)
