# -*- coding: utf-8 -*-

import logging
import signal
import socket
import traceback

from amqp import RecoverableConnectionError
from easyjoblite.utils import enqueue, is_main_thread
from kombu import Connection
from kombu import Consumer
from kombu import Producer


class BaseRMQConsumer(object):
    """
    base class for all rabbit-mq workers that consume from any of the job queues
    """

    def __init__(self, orchestrator):
        """
        :param configuration: all necessary configuration needed for the worker
        """
        logger = logging.getLogger(self.__class__.__name__)

        try:
            # Doing variable init
            self._orchestrator = orchestrator
            if is_main_thread():
                logger.info("Adding signal handler.")
                signal.signal(signal.SIGTERM, self.signal_term_handler)
            self._should_block = True
            self._from_queue = None
            self._is_connection_reset = False

            # starting connection
            self.start_connection()

        except Exception as e:
            traceback.print_exc()
            logger.error("Error connecting to rabbitmq({u}): {err}".format(u=self.get_config().rabbitmq_url,
                                                                           err=e.message))
            raise

    def get_config(self):
        return self._orchestrator.get_config()

    def should_run_loop(self):
        return self._should_block

    def start_connection(self):
        """
        reset the connection to rabbit mq
        :return: 
        """
        logger = logging.getLogger(self.__class__.__name__)
        logger.info("starting new rabbit mq connection")

        # todo: do we need to make confirm_publish configurable?
        self._conn = Connection(self.get_config().rabbitmq_url,
                                transport_options={'confirm_publish': True})

        # setup producer to push to error and dlqs
        self._producer = Producer(channel=self._conn.channel(),
                                  exchange=self._orchestrator.get_exchange())

    def start_rmq_consume(self):
        """
        start consuming from rmq
        :return: 
        """
        logger = logging.getLogger(self.__class__.__name__)
        logger.info("starting rabbit mq consumer")

        channel = self._conn.channel()

        # prep a consumer for the from_queue only
        self._queue_consumer = Consumer(channel=channel,
                                        queues=[self._from_queue],
                                        callbacks=[self.process_message])
        self._queue_consumer.consume()

    def rmq_reset(self):
        """
        reset the rmq relate services
        :return: 
        """
        logger = logging.getLogger(self.__class__.__name__)
        logger.info("rabbit mq connection reset called")

        self.start_connection()
        self.start_rmq_consume()
        self._is_connection_reset = True

    def consume(self, from_queue, blocking=True):
        """
        consumes from from_queue and feeds each of the message into
        process_message callback. this is a blocking call

        todo: this can later be ended using a poison-pill

        :param from_queue: kombu.Queue to consume from
        :param blocking: should it block waiting for new messages?
        :return: nothing, might raise exception/s
        """

        logger = logging.getLogger(self.__class__.__name__)
        self._from_queue = from_queue

        # start consuming from rmq
        self.start_rmq_consume()

        # drain the events into the consumer
        logger.info("starting to consume messages from {exchg}:{q}".format(exchg=from_queue.exchange.name,
                                                                           q=from_queue.name))

        while self.should_run_loop():
            try:
                if blocking:
                    self._conn.drain_events()

                else:
                    # at max we wait for 5 more seconds after exhausting all work-items from
                    # the queue.
                    try:
                        self._conn.drain_events(timeout=1)

                    except socket.timeout:
                        logger.debug("No more work-items in {q}".format(q=from_queue.name))
                        break

                    except socket.error as e:
                        # we don't care about EAGAIN, since we had intentionally started a non-blocking conn
                        if e.errno == 35:
                            msg = "{q} is empty".format(q=from_queue.name)
                            logger.debug(msg)

                        else:
                            traceback.print_exc()
                            logger.error(e)

                        break
            except (IOError, KeyboardInterrupt) as e:
                logger.error("Got io error so shutting down.".format(err=e.message))
                self._should_block = False
            except Exception as e:
                logger.warning("Exception happened may be connection reset.")
                if not self._is_connection_reset:
                    traceback.print_exc()
                    logger.error(
                        "Something broke while listening for new messages: {err}".format(err=e.message))
                    self._should_block = False
                self._is_connection_reset = False

        self._queue_consumer.cancel()

        # disconnect from the queue
        logger.info("Quitting from locked listening")

    def produce_to_queue(self, queue_type, body, job):
        """
        produce message to the relevant queue
        :param queue_type: type of the queue
        :param body: the body of the message
        :param job: the job 
        :return: 
        """
        logger = logging.getLogger(self.__class__.__name__)
        logger.info("Enquiue go for job-id: {} into queue: {}".format(job.id, queue_type))

        retry_count = 0
        max_retry_count = 3

        while retry_count < max_retry_count:
            try:
                enqueue(self._producer, queue_type, job, body)
                break
            except RecoverableConnectionError as e:
                logger.error(
                    "RecoverableConnectionError exception while enqueuing so resetting connection: {err}".format(
                        err=e.message))
                self.rmq_reset()
                retry_count += 1
            except Exception as e:
                traceback.print_exc()
                logger.error("Unknown exception while enqueuing: {err}".format(err=e.message))
                raise

    def signal_term_handler(self, signum, frame):
        """
        signal handler for the workers
        :param signum: 
        :param frame: 
        :return: 
        """
        logger = logging.getLogger(self.__class__.__name__)
        logger.warning("SIGTERM found so stopping worker with signum {0}".format(signum))
        self._should_block = False

    def process_message(self, body, message):
        """
        gets called back once a message arrives in the work queue

        :param body: message payload
        :param message: queued message with headers and other metadata
        """
        raise NotImplementedError("'{n}' needs to implement process_message(...)".format(n=self.__class__.__name__))
