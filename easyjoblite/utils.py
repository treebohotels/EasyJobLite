# -*- coding: utf-8 -*-
import logging
import pickle
import signal
import sys

import os
from kombu.entity import PERSISTENT_DELIVERY_MODE


def as_text(v):
    if v is None:
        return None
    elif isinstance(v, bytes):
        return v.decode('utf-8')
    elif isinstance(v, str):
        return v
    else:
        raise ValueError('Unknown type %r' % type(v))


def is_string_type(value):
    string_types = (str, unicode)
    if isinstance(value, string_types):
        return True
    else:
        return False


def save_obj(obj, path):
    """
    save a python object to a file
    :param obj: the object to be saved
    :param path: the path of the file where to be saved
    :return: None
    """
    with open(path, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(path):
    """
    return the python object saved in the given path
    :param path: the path to be loaded
    :return: 
    """
    with open(path, 'rb') as f:
        return pickle.load(f)


def kill_process(pid):
    """
    kill a process for a given pid
    :param pid: pid to be killed
    :return: True if killed sucessfully or raises an exception
    """
    try:
        os.kill(int(pid), signal.SIGTERM)
        # Check if the process that we killed is alive.
    except OSError as ex:
        return True


def kill_workers(service_state, type):
    """
    function to kill all the workers of the given type
    :param service_state: current state of the service
    :param type: the type of the worker to kill
    :return: 
    """
    logger = logging.getLogger("kill_workers")
    logger.info("Started killing : " + type + " with list " + str(service_state.get_pid_list(type)))
    pid_list = list(service_state.get_pid_list(type))
    for pid in pid_list:
        kill_process(pid)
        service_state.remove_worker_pid(type, pid)
        logging.info("Done killing : " + str(pid))


def update_import_paths(import_paths):
    """
    update the import paths in the system
    :param import_paths: 
    :return: 
    """
    if import_paths:
        sys.path = import_paths.split(':') + sys.path


def enqueue(producer, queue_type, job, body):
    """
    enque a job in the given queue
    :param producer: the producer to be used
    :param queue_type: type of queue (worker, retry, dead)
    :param job: the job object
    :param body: the body payload of the job
    :return: none
    """
    routing_key = "{type}.{tag}".format(type=queue_type, tag=job.tag)
    headers = job.to_dict()
    producer.publish(body=body,
                     headers=headers,
                     routing_key=routing_key,
                     delivery_mode=PERSISTENT_DELIVERY_MODE)
