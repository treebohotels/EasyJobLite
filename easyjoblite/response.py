# -*- coding: utf-8 -*-


class EasyResponse(object):
    """ Response object for the job run"""

    def __init__(self, status_code, response_message=None, response_data=None):
        self.status_code = status_code
        self.message = response_message
        self.data = response_data
