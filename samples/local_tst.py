# -*- coding: utf-8 -*-
from easyjoblite.response import EasyResponse


def local_method_job_failed(data):
    '''
    this function is called as part of the job
    :param data: 
    :return: 
    '''
    data_str = "Local method job recieved in local_method_job_failed with data: " + str(data)
    print(data_str)
    fo = open("job.txt", "a")
    fo.write(data_str)
    fo.close()
    return EasyResponse(500, "opps")


def got_error(data):
    data_str = "Error recieved with data " + str(data)
    fo = open("error.txt", "wb")
    fo.write(data_str)
    fo.close()
    print(data_str)


class LocalTestClass(object):
    def local_instance_job_failed(data):
        '''
        this function is called as part of the job
        :param data: 
        :return: 
        '''
        data_str = "Local method job recieved in local_instance_job_failed with data: " + str(data)
        print(data_str)
        fo = open("job.txt", "wb")
        fo.write(data_str)
        fo.close()
        return {"status_code": 500}
