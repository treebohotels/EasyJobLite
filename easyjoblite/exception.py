# -*- coding: utf-8 -*-


class ApiTimeoutException(Exception):
    def __init__(self, api, timeout):
        """
        raised when the Job execution is timedout
        
        :param api: Api which timedout
        :param timeout: timeout value
        """
        self.api = api
        self.timeout = timeout

        message = "Request {url} timed out after {t}s".format(url=self.api, t=self.timeout)

        super(ApiTimeoutException, self).__init__(message)


class EasyJobServiceNotStarted(Exception):
    def __init__(self):
        """
        raised when the service is not started
        """

        message = "The easy job service is not started."

        super(EasyJobServiceNotStarted, self).__init__(message)


class UnableToCreateJob(Exception):
    def __init__(self, data):
        """
        raised when unable to create an easyjob
        """

        self.data = data

        message = "Unable to create job."

        super(UnableToCreateJob, self).__init__(message)
