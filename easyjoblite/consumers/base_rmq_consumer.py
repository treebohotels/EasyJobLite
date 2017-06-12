# -*- coding: utf-8 -*-

import logging
import signal
import socket
import traceback

from easyjoblite.utils import enqueue
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
            self._orchestrator = orchestrator
            # todo: do we need to make confirm_publish configurable?
            self._conn = Connection(self.get_config().rabbitmq_url,
                                    transport_options={'confirm_publish': True})

            # setup producer to push to error and dlqs
            self._producer = Producer(channel=self._conn.channel(),
                                      exchange=self._orchestrator.get_exchange())
            signal.signal(signal.SIGTERM, self.signal_term_handler)
            self._should_block = True

        except Exception as e:
            traceback.print_exc()
            logger.error("Error connecting to rabbitmq({u}): {err}".format(u=self.get_config().rabbitmq_url,
                                                                           err=e.message))
            raise

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
        channel = self._conn.channel()

        # prep a consumer for the from_queue only
        queue_consumer = Consumer(channel=channel,
                                  queues=[from_queue],
                                  callbacks=[self.process_message])
        queue_consumer.consume()

        try:
            # drain the events into the consumer
            logger.info("starting to consume messages from {exchg}:{q}".format(exchg=from_queue.exchange.name,
                                                                               q=from_queue.name))
            while self.should_run_loop():
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

        except Exception as e:
            traceback.print_exc()
            logger.error("Something broke while listening for new messages: {err}".format(err=e.message))
            logger.info("Quitting")

        # disconnect from the queue
        queue_consumer.cancel()

    def process_message(self, body, message):
        """
        gets called back once a message arrives in the work queue

        :param body: message payload
        :param message: queued message with headers and other metadata
        """
        raise NotImplementedError("'{n}' needs to implement process_message(...)".format(n=self.__class__.__name__))

    def produce_to_queue(self, type, body, job):
        enqueue(self._producer, type, job, body)

    def get_config(self):
        return self._orchestrator.get_config()

    def should_run_loop(self):
        return self._should_block

    def signal_term_handler(self, signum, frame):
        logger = logging.getLogger(self.__class__.__name__)
        logger.debug("SIGTERM found so stopping worker")
        self._should_block = False
