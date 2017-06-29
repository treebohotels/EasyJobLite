# -*- coding: utf-8 -*-

import logging
import time
from easyjoblite import orchestrator, constants
from local_tst import local_method_job_failed

logging.basicConfig(level=logging.INFO)


def test_job_work():
    worker = orchestrator.Orchestrator(rabbitmq_url='amqp://guest:guest@localhost:5672//',
                                       eqc_sleep_duration=1, config_file="../templates/easyjoblite.yaml")
    print "RABBIT MQ URL: " + str(worker.get_config().rabbitmq_url)
    print "SLEEP TIME: " + str(worker.get_config().eqc_sleep_duration)
    print "MAX RETRIES: " + str(worker.get_config().max_retries)

    worker.setup_entities()
    print str(worker.get_config().__dict__)
    data = {"test1": "test2"}
    for i in range(100):
        print "sending to queue"
        try:
            worker.enqueue_job(local_method_job_failed, constants.API_LOCAL, data=data)
        except Exception as e:
            print "Got exception"

        time.sleep(2)

    worker.start_service()
    print "Done enquing job"


test_job_work()
