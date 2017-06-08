# -*- coding: utf-8 -*-

import logging
import traceback

from base_rmq_consumer import BaseRMQConsumer
from easyjoblite.job import EasyJob


class DeadLetterQueueConsumer(BaseRMQConsumer):
    """
    dead letter consumes from different queue. The reason for a consumer for Dead letter queue
    was to send notifications for manual interventions
    """

    def consume_from_dead_letter_queue(self, queue):
        self.consume(queue)

    def process_message(self, body, message):
        """
        gets called back once a message arrives in the work queue

            1. Sends email notifiactions once msg goes to dead letter queue

        :param body: message payload
        :param message: queued message with headers and other metadata
        """
        logger = logging.getLogger(self.__class__.__name__)
        job = EasyJob.create_from_dict(message.headers)

        try:
            response = job.notify_error(body, self.get_config().async_timeout)

            if response.status_code != 200:
                logger.error("Failed to notify with status code: {} with message: {}".format(response.status_code,
                                                                                             response.message))
                self.log_to_file(response, job, body)
        except Exception as e:
            traceback.print_exc()
            err_msg = "Notification Failed for job {}".format(body)
            logger.error(err_msg)

        message.ack()

    def log_to_file(self, response, job, data):
        """
        log the data to the dl queue error log file
        :param response: the response to be logged
        :param job: job to be logged
        :param data: data to be logged
        :return: 
        """
        response_dict = {"job": job.to_dict(), "data": data, "response": response.__dict__}
        log_file = open(self._orchestrator.get_config().dead_message_log_file, "w")
        log_file.write(str(response_dict))
        log_file.write("\n")
        log_file.close()
