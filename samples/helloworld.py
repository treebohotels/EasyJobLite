# -*- coding: utf-8 -*-

import logging

from easyjoblite import orchestrator, constants
from local_tst import local_method_job_failed

logging.basicConfig()


def test_job_work():
    worker = orchestrator.Orchestrator(rabbitmq_url='amqp://guest:guest@localhost:5672//',
                                       eqc_sleep_duration=1, config_file="../templates/easyjoblite.yaml")
    print "RABBIT MQ URL: " + str(worker.get_config().rabbitmq_url)
    print "SLEEP TIME: " + str(worker.get_config().eqc_sleep_duration)
    print "MAX RETRIES: " + str(worker.get_config().max_retries)

    worker.start_service()
    print str(worker.get_config().__dict__)
    data = {"test1": "test2"}
    worker.enqueue_job(local_method_job_failed, constants.API_LOCAL, data=data)
    print "Done enquing job"


test_job_work()
