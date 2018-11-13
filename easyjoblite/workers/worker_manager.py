import logging

from easyjoblite import state, constants
from easyjoblite.utils import kill_process

logger = logging.getLogger(__name__)


class WorkerManager(object):

    @staticmethod
    def stop_all_workers(worker_type):
        """
        stops all the workers of the given type
        :param worker_type: 
        :return: 
        """
        logger = logging.getLogger("stop_all_workers")
        service_state = state.ServiceState()
        worker_type_list = [constants.WORK_QUEUE, constants.RETRY_QUEUE, constants.DEAD_LETTER_QUEUE]

        if worker_type in worker_type_list:
            WorkerManager.kill_workers(service_state, worker_type)
            logger.info("Done stopping all the workers of worker_type {}".format(worker_type))
        elif worker_type == constants.STOP_TYPE_ALL:
            for local_type in worker_type_list:
                WorkerManager.kill_workers(service_state, local_type)
            logger.info("Done stopping all the workers ")
        else:
            raise KeyError
        service_state.refresh_all_workers_pid()

    @staticmethod
    def kill_workers(service_state, type):
        """
        function to kill all the workers of the given type
        :param service_state: current state of the service
        :param type: the type of the worker to kill
        :return: 
        """
        logger.info("Started killing : " + type + " with list " + str(service_state.get_pid_list(type)))
        pid_list = list(service_state.get_pid_list(type))
        for pid in pid_list:
            kill_process(pid)
            logging.info("Done killing : " + str(pid))
