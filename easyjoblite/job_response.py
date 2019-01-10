from enum import Enum


class JobResponse(Enum):
    SUCCESS = 200
    IGNORE_RESPONSE_AND_RETRY = 300
    NON_RETRYABLE_FAILURE = 400
    RETRYABLE_FAILURE = 500
