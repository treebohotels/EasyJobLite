Easy Job lite is a simple Python library for queueing jobs and processing
them in the background with workers.  It is backed by RabbitMQ and it is designed to support retial and deal letter management.

## Requirement

 rabbitMQ

## Getting started

First, run a rabbitMQ server, of course:

```console
$ rabbitmq-server
```

The library supports two mode of operation. remote jobs and local jobs.
To put jobs on queues (for remote):

```python
import easyjoblite.orchestrator
import easyjoblite.constants

def test_job_work():
    worker = orchestrator.Orchestrator(rabbitmq_url='amqp://guest:guest@localhost:5672//')
    worker.start_service()
    api = "https://test.domain.com/booking_conform"
    notification_handler = "https://test.domain.com/failure_listner"
    data = {"hotel_id": "DL001", "room_id": 101, "user_id": 12321}
    worker.enqueue_job(api, constants.API_REMOTE, data=data, notification_handler=notification_handler)
```
And you are done.

For a more complete example, refer to the samples folder in the repo.  But this is the essence.

## Installation

Simply use the following command to install the latest released version:

    pip install easyjoblite


## Project history

This project has been inspired by the good parts of [Celery][1], [RabbitMQ][2]
and [Reddis Queue][3], and has been created as a lightweight alternative to the
heaviness of Celery or other AMQP-based queueing implementations.


[1]: http://www.celeryproject.org/
[2]: https://www.rabbitmq.com
[3]: http://python-rq.org/