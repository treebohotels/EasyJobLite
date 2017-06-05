# -*- coding: utf-8 -*-

import logging
import traceback

import easyjoblite.exception
from base_rmq_consumer import BaseRMQConsumer
from easyjoblite import constants
from easyjoblite.job import EasyJob


class WorkQueueConsumer(BaseRMQConsumer):
    """
    worker consumes from worker queue, calls underlying crs booking apis, and
    based on error responses decides whether to retry, how many times, and what
    to do if the work item fails too many times
    """

    def consume_from_work_queue(self, queue):
        """
        starts the process of consuming jobs from the queue
        :param queue: the queue from which to consume
        :return: NA
        """
        self.consume(queue)

    def process_message(self, body, message):
        """
        gets called back once a message arrives in the work queue

            1. calls embedded api with the payload as its parameters when a message arrives

            2. if the call is successful, acks the message
            3. for remote call 
                a. in case the call fails with 4XX, just acks the message, no further action
                b. in case the call fails with a 5XX,
                  - adds the error to error-log header
                  - if num-retries are more than max_retries,
                    - puts the message in dead-letter-queue
                  - else
                    - increases num-retries by 1
                    - puts the message in error-queue
            4. for local call
                a. in case the call fails with a exception then adds the call to a dead letter queue

        :param body: message payload
        :param message: queued message with headers and other metadata (contains a EasyJob object in headers)
        """
        logger = logging.getLogger(self.__class__.__name__)

        try:
            job = EasyJob.create_from_dict(message.headers)
            api = job.api
            logger.debug("recieved api: " + str(api))

            response = job.execute(body, self.get_config().async_timeout)

            if response.status_code >= 400:
                # todo: we should have booking id here in the log message
                logger.info("{status}: {resp}".format(status=response.status_code,
                                                      resp=response.message))

                if response.status_code >= 500:
                    # we have a retry-able failure
                    self._push_message_to_error_queue(body, message, response.message)

                # Check if the error is set to be notified then add to dlq
                if job.should_notify_error():
                    self._push_msg_to_dlq(body=body,
                                          message=message,
                                          err_msg=response.message,
                                          )
        except (Exception, easyjoblite.exception.ApiTimeoutException) as e:
            traceback.print_exc()
            logger.error(e.message)
            self._push_message_to_error_queue(body, message, e.message)
            return
        except easyjoblite.exception.UnableToCreateJob as e:
            logger.error(e.message + " data: " + str(e.data))
            self.__push_raw_msg_to_dlq(body=body,
                                       message=message,
                                       err_msg=e.message,
                                       )

        message.ack()

    def __push_raw_msg_to_dlq(self, body, message, err_msg):
        """
        pushes the raw message to dead letter queue for manual intervension and notification

        :param body: body of the message
        :param message: kombu amqp message object with headers and other metadata
        :param error_mesg: what error caused this push to error queue
        """
        logger = logging.getLogger(self.__class__.__name__)

        try:
            logger.info("Moving raw item to DLQ for notification and manual intervention")
            job = EasyJob()
            job.data = message.headers
            job.add_error(err_msg)
            self.produce_to_queue(constants.DEAD_LETTER_QUEUE, body, job)

        except Exception as e:
            traceback.print_exc()
            logger.error("Error moving the work-item to dead-letter-queue: {err}".format(err=e.message))

    def _push_msg_to_dlq(self, body, message, err_msg):
        """
        pushes the message to dead letter queue for manual intervension and notification

        :param body: body of the message
        :param message: kombu amqp message object with headers and other metadata
        :param error_mesg: what error caused this push to error queue
        """
        logger = logging.getLogger(self.__class__.__name__)

        try:
            job = EasyJob.create_from_dict(message.headers)
            logger.info("Moving item to DLQ for notification and manual intervention")
            job.add_error(err_msg)
            self.produce_to_queue(constants.DEAD_LETTER_QUEUE, body, job)

        except Exception as e:
            traceback.print_exc()
            logger.error("Error moving the work-item to dead-letter-queue: {err}".format(err=e.message))
            self.__push_raw_msg_to_dlq(body, message, err_msg)

    def _push_message_to_error_queue(self, body, message, error_mesg):
        """
        pushes the message to appropriate error queue based on number of
        retries on the message so far

        :param body: body of the message
        :param message: kombu amqp message object with headers and other metadata
        :param error_mesg: what error caused this push to error queue
        """
        logger = logging.getLogger(self.__class__.__name__)

        headers = message.headers
        job = EasyJob.create_from_dict(message.headers)

        if job.no_of_retries < self.get_config().max_retries:
            # we are allowed more retries, so move this to error queue
            logger.debug("Moving work-item {t}:'{d}' to error-queue for retry later".format(t=job.tag,
                                                                                            d=body))
            try:
                job.increment_retries()
                job.add_error(error_mesg)

                self.produce_to_queue(constants.RETRY_QUEUE, body, job)

            except Exception as e:
                traceback.print_exc()
                logger.error("Error moving the work-item to error-queue: {err}".format(err=e.message))
                self.__push_raw_msg_to_dlq(body, message, e.message)
        else:
            er_message = "Max retries exceeded, moving work-item to DLQ for manual intervention: " + error_mesg
            logger.info(error_mesg)
            self._push_msg_to_dlq(body, message, er_message)
