# -*- coding: utf-8 -*-

import constants
import os
import utils


class ServiceState(object):
    def __init__(self, pid_file_path=constants.DEFAULT_PID_FILE_LOCATION):
        self.job_worker_pids = []
        self.retry_worker_pids = []
        self.dlq_worker_pids = []
        self.pid_file = pid_file_path
        if os.path.exists(self.pid_file):
            tmp_obj = utils.load_obj(self.pid_file)
            self.__dict__ = tmp_obj.__dict__
        else:
            self.persist_state()

    def persist_state(self):
        utils.save_obj(self, self.pid_file)

    def get_pid_list(self, type):
        if type == constants.WORK_QUEUE:
            return self.job_worker_pids
        elif type == constants.RETRY_QUEUE:
            return self.retry_worker_pids
        elif type == constants.DEAD_LETTER_QUEUE:
            return self.dlq_worker_pids
        else:
            raise KeyError("Invalid key : " + str(type))

    def add_worker_pid(self, type, pid):
        if type == constants.WORK_QUEUE:
            self.job_worker_pids.append(pid)
        elif type == constants.RETRY_QUEUE:
            self.retry_worker_pids.append(pid)
        elif type == constants.DEAD_LETTER_QUEUE:
            self.dlq_worker_pids.append(pid)
        else:
            raise KeyError("Invalid key : " + str(type))
        self.persist_state()

    def remove_worker_pid(self, type, pid):
        if type == constants.WORK_QUEUE:
            self.job_worker_pids.remove(pid)
        elif type == constants.RETRY_QUEUE:
            self.retry_worker_pids.remove(pid)
        elif type == constants.DEAD_LETTER_QUEUE:
            self.dlq_worker_pids.remove(pid)
        else:
            raise KeyError("Invalid key : " + str(type))
        self.persist_state()
