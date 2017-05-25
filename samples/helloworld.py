# -*- coding: utf-8 -*-

import logging

from easyjoblite import orchestrator, constants
from local_tst import got_error
from local_tst import local_method_job_failed

logging.basicConfig()


def test_job_work():
    worker = orchestrator.Orchestrator(rabbitmq_url='amqp://guest:guest@localhost:5672//')
    worker.start_service()
    data = {"test1": "test2"}
    worker.enqueue_job(local_method_job_failed, constants.API_LOCAL, data=data, notification_handler=got_error)


test_job_work()
