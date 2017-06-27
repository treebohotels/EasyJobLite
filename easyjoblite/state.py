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
        """
        Persist the library pid state to the pid file
        :return: 
        """
        utils.save_obj(self, self.pid_file)

    def get_pid_list(self, type):
        """
        Get the pid list for the give type of worker
        :param type: type of worker
        :return: 
        """
        if type == constants.WORK_QUEUE:
            return self.job_worker_pids
        elif type == constants.RETRY_QUEUE:
            return self.retry_worker_pids
        elif type == constants.DEAD_LETTER_QUEUE:
            return self.dlq_worker_pids
        else:
            raise KeyError("Invalid key : " + str(type))

    def add_worker_pid(self, type, pid):
        """
        add a pid for the worker
        :param type: type of worker
        :param pid: pid to be added
        :return: 
        """
        if type == constants.WORK_QUEUE:
            self.job_worker_pids.append(pid)
        elif type == constants.RETRY_QUEUE:
            self.retry_worker_pids.append(pid)
        elif type == constants.DEAD_LETTER_QUEUE:
            self.dlq_worker_pids.append(pid)
        else:
            raise KeyError("Invalid key : " + str(type))
        self.persist_state()

    def remove_worker_pid(self, worker_type, pid):
        """
        remove a worker pid from the list
        :param worker_type: type of worker
        :param pid: pid to be removed
        :return: 
        """
        if worker_type == constants.WORK_QUEUE:
            self.job_worker_pids.remove(pid)
        elif worker_type == constants.RETRY_QUEUE:
            self.retry_worker_pids.remove(pid)
        elif worker_type == constants.DEAD_LETTER_QUEUE:
            self.dlq_worker_pids.remove(pid)
        else:
            raise KeyError("Invalid key : " + str(worker_type))
        self.persist_state()

    def refresh_workers_pid(self, worker_type):
        """
        refresh the state of pids
        :param worker_type: the type of worker
        :return: 
        """
        pid_list = list(self.get_pid_list(worker_type))
        # job worker
        for pid in pid_list:
            if not utils.is_process_running(pid):
                self.remove_worker_pid(worker_type, pid)

    def refresh_all_workers_pid(self):
        """
        refresh all the worker queue pids
        :return: 
        """
        self.refresh_workers_pid(constants.WORK_QUEUE)
        self.refresh_workers_pid(constants.RETRY_QUEUE)
        self.refresh_workers_pid(constants.DEAD_LETTER_QUEUE)
