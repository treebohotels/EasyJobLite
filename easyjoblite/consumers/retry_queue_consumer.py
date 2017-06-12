# -*- coding: utf-8 -*-

import logging
import socket
import time
import traceback

from base_rmq_consumer import BaseRMQConsumer
from easyjoblite import constants
from easyjoblite.job import EasyJob
from kombu import Consumer


class RetryQueueConsumer(BaseRMQConsumer):
    """
    consumes from error queue, checks how many times the work item has been
    retried; if num-retries are less than max-retries, moves the work-item
    back into work-queue; else moves the work item into dlq

    IMP NOTE: sleeps for 15 mins between recurrent activity (as mentioned above)
    """

    def consume_from_retry_queue(self, queue, buffer_queue):
        logger = logging.getLogger(self.__class__.__name__)
        while True:
            # first shovel all messages from error-queue to buffer queue
            self._shovel_to_buffer(queue)

            # directly consume from error queue, but don't block
            logger.info("checking {q} for work items ...".format(q=queue.name))
            self.consume(buffer_queue, blocking=False)

            # sleep for a pre-defined time
            logger.info("sleeping for {m} mins ...".format(m=self.get_config().eqc_sleep_duration))
            time.sleep(self.get_config().eqc_sleep_duration * 60)

            if not self.should_run_loop():
                break

    def process_message(self, body, message):
        """
        gets called back for every message in the error queue

        :param body: message payload
        :param message: queued message with headers and other metadata
        """
        logger = logging.getLogger(self.__class__.__name__)

        job = EasyJob.create_from_dict(message.headers)

        if job.no_of_retries < self.get_config().max_retries:
            # we are allowed more retries, so move this to error queue
            logger.debug("Moving work-item {t}:'{d}' back to work-queue for retry".format(t=job.tag, d=body))

            try:
                self.produce_to_queue(constants.WORK_QUEUE, body, job)

                message.ack()

            except Exception as e:
                traceback.print_exc()
                logger.error("Error moving the work-item to error-queue: {err}".format(err=e.message))
                # todo: what do we do next in this case?

        else:
            # no more retries, we have had enough. push the message to dlq for manual intervention
            logger.info(
                "Max retries exceeded, moving work-item {t}:{d} to DLQ for manual intervention".format(t=job.tag,
                                                                                                       d=body))

            try:
                self.produce_to_queue(constants.DEAD_LETTER_QUEUE, body, job)

                message.ack()

            except Exception as e:
                traceback.print_exc()
                logger.error("Error moving the work-item to dead-letter-queue: {err}".format(err=e.message))
                # todo: what do we do next in this case?

    def _shoveller(self, body, message):
        """
        callback that does the actual shovelling
        :param body: body of the work-item
        :param message: RMQ message object
        """
        logger = logging.getLogger(self.__class__.__name__)

        headers = message.headers
        job = EasyJob.create_from_dict(headers)

        self.produce_to_queue(constants.BUFFER_QUEUE, body, job)

        message.ack()

        logger.debug("shoveller: moved {t}:'{d}' work-item to buffer queue".format(t=job.tag, d=body))

    def _shovel_to_buffer(self, from_queue):
        """
        poor man's alternative to the shovel plugin

        :param from_queue: shovel messages from which queue? entity.Queue object
        """
        logger = logging.getLogger(self.__class__.__name__)
        logger.info("shovelling all messages from error queue to buffer queue")

        channel = self._conn.channel()

        # prep a consumer for the from_queue only
        queue_consumer = Consumer(channel=channel,
                                  queues=[from_queue],
                                  callbacks=[self._shoveller])
        queue_consumer.consume()

        # finally drain all the work items from error-queue into shoveller
        while True:
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
                    break

        # disconnect
        queue_consumer.cancel()
