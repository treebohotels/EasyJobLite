# -*- coding: utf-8 -*-

import logging
import os
import pickle
import signal
import sys
import threading
import time
import traceback

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
    string_types = (str)
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
    logger = logging.getLogger("load_obj")
    retry_count = 3
    while retry_count > 0:
        try:
            with open(path, 'rb') as f:
                return pickle.load(f)
        except (IOError, EOFError) as e:
            logger.warning("failed loading obj with error: {}. Will retry for {} time.".format(retry_count, str(e)))
            time.sleep(2)
            retry_count -= 1


def is_process_running(pid):
    """Check whether pid exists in the current process table.
    UNIX only.
    :param pid: the processes id to check
    :return: TRUE/FALSE
    """
    if pid < 0:
        return False
    try:
        ret = os.popen("ps -eo pid,stat | grep -w {} |  grep -v \'Z\' | awk \'{{ print $1}}\'".format(pid)).read()

        if ret and int(ret) == pid:
            return True
        else:
            return False
    except Exception as e:
        traceback.print_exc()
        logger = logging.getLogger("is_process_running")
        logger.exception("is_process_running")
        logger.warning("something broke while getting process state so taking it as running.")
        return True


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


def get_pid_state_string(pid_list):
    """
    state of the pids in string
    :param pid_list: list of pid's
    :return: 
    """
    total_state = ""

    for pid in pid_list:
        state_str = "STOPPED"
        if is_process_running(pid):
            state_str = "RUNNING"

        total_state += "{}:{}   ".format(pid, state_str)

    if not total_state:
        return "NONE"
    return total_state


def is_main_thread():
    """
    check if current thread is main thread or not
    :return: 
    """
    return threading.current_thread().__class__.__name__ == '_MainThread'


def update_import_paths(import_paths):
    """
    update the import paths in the system
    :param import_paths: 
    :return: 
    """
    if import_paths:
        sys.path = import_paths.split(':') + sys.path


def enqueue(producer, queue_type, job, body, retry, retry_policy=None):
    """
    enque a job in the given queue
    :param producer: the producer to be used
    :param queue_type: type of queue (worker, retry, dead)
    :param job: the job object
    :param body: the body payload of the job
    :param retry: enable rabbitmq retry
    :param retry_policy: rabbitmq retry policy
    :return: none
    """
    routing_key = "{type}.{tag}".format(type=queue_type, tag=job.tag)
    headers = job.to_dict()
    if retry:
        producer.publish(body=body,
                         headers=headers,
                         routing_key=routing_key,
                         retry=retry,
                         retry_policy=retry_policy,
                         delivery_mode=PERSISTENT_DELIVERY_MODE)
    else:
        producer.publish(body=body,
                         headers=headers,
                         routing_key=routing_key,
                         delivery_mode=PERSISTENT_DELIVERY_MODE)
