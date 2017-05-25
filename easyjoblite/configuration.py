# -*- coding: utf-8 -*-

import constants


class Configuration(object):
    """
    used to configure the service
    """

    def __init__(self,
                 rabbitmq_url=constants.DEFAULT_RMQ_URL,
                 max_retries=constants.DEFAULT_MAX_JOB_RETRIES,
                 async_timeout=constants.DEFAULT_ASYNC_TIMEOUT,
                 eqc_sleep_duration=constants.DEFAULT_ERROR_Q_CON_SLEEP_DURATION,
                 max_worker_count=constants.DEFAULT_MAX_WORKER_COUNT,
                 default_worker_count=constants.DEFAULT_WORKER_COUNT,
                 default_retry_consumer_count=constants.DEFAULT_RETRY_CONSUMER_COUNT,
                 default_dl_consumer_count=constants.DEFAULT_DLQ_CONSUMER_COUNT,
                 import_paths=constants.DEFAULT_IMPORT_PATHS,
                 pid_file_path=constants.DEFAULT_PID_FILE_LOCATION,
                 workers_log_file_path=constants.DEFAULT_LOG_FILE_PATH):
        """
        
        :param rabbitmq_url: where it the rabbitmq server running?
        :param max_retries: max retries before a message moves to DLQ
        :param async_timeout: wait these many seconds before we timeout async reqs
        :param eqc_sleep_duration: how long (in mins) should error-queue-consumer sleep between bursts
        :param max_worker_count: max ammount of job workers
        :param default_worker_count: default about of job worker to be spawned
        :param default_retry_consumer_count: default amount of retry consumer to be spawned
        :param default_dl_consumer_count: default amount of dead letter consumer to be spawned
        :param import_paths: the paths for looking to import modules for local run
        :param pid_file_path: the path to store the pid files of all the workers
        """

        # todo: do we need to validate the URL is in proper format
        self.rabbitmq_url = rabbitmq_url
        self.max_retries = max_retries
        self.async_timeout = async_timeout
        self.eqc_sleep_duration = eqc_sleep_duration
        self.max_worker_count = max_worker_count
        self.default_worker_count = default_worker_count
        self.default_retry_consumer_count = default_retry_consumer_count
        self.default_dl_consumer_count = default_dl_consumer_count
        self.import_paths = import_paths
        self.pid_file_path = pid_file_path
        self.workers_log_file_path = workers_log_file_path
