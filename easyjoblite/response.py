# -*- coding: utf-8 -*-


class EasyResponse(object):
    """ Response object for the job run"""

    def __init__(self, status_code, message=None, data=None):
        self.status_code = status_code
        self.message = message
        self.data = data
