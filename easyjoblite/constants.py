# -*- coding: utf-8 -*-

import requests

# Job types
JOB_TYPE = 'job_type'
API_LOCAL = 'local'
API_REMOTE = 'remote'
job_call_types = {API_REMOTE, API_LOCAL}

# Remote call type
REMOTE_CALL_TYPE = 'remote_call_type'
remote_call_type = {
    'post': requests.post,
    'get': requests.get,
    'put': requests.put,
    'delete': requests.delete
}

# Errors to be notified
errors_to_be_notified = {
    'booking.confirm': {400, 410},
    'booking.cancel': {400}
}

# Queue Types
EXCHANGE = "exchange"
DEAD_LETTER_QUEUE = "dead"
RETRY_QUEUE = "retry"
WORK_QUEUE = "work"
BUFFER_QUEUE = "buffer"


# rabbit mq exchange and queue prefix
rabbit_mq_prefix = {
    EXCHANGE: "-booking-exchange",
    WORK_QUEUE: "-work-queue",
    DEAD_LETTER_QUEUE: "-dead-letter-queue",
    RETRY_QUEUE: "-retry-queue",
    BUFFER_QUEUE: "-buffer-queue",
}

# Default configs
DEFAULT_APP_ID = "DefaultApp"
DEFAULT_RMQ_URL = 'amqp://guest:guest@localhost:5672//'
DEFAULT_ASYNC_TIMEOUT = 120
DEFAULT_MAX_JOB_RETRIES = 3
DEFAULT_ERROR_Q_CON_SLEEP_DURATION = 15
DEFAULT_MAX_WORKER_COUNT = 10
DEFAULT_WORKER_COUNT = 3
DEFAULT_RETRY_CONSUMER_COUNT = 1
DEFAULT_DLQ_CONSUMER_COUNT = 3
DEFAULT_IMPORT_PATHS = '.'
DEFAULT_PID_FILE_LOCATION = "/var/tmp/easyjoblite.pid"
DEFAULT_LOG_FILE_PATH = "/var/tmp/easyjoblite.log"
DEFAULT_DL_LOG_FILE = "/var/tmp/easyjoblite_dl.log"
DEFAULT_CONFIG_FILE = "/var/tmp/easyjoblite.yaml"

BASE_COMMAND = "nohup easyjoblite"
STOP_TYPE_ALL = "all"
