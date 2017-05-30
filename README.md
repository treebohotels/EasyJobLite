# Easy Job lite
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
To put jobs on queues:

#### Local job example

```python
from easyjoblite import orchestrator, constants
from local_tst import got_error
from local_tst import local_method_job

def test_job_work():
    worker = orchestrator.Orchestrator(rabbitmq_url='amqp://guest:guest@localhost:5672//')
    worker.start_service()
    data = {"test1": "test2"}
    worker.enqueue_job(local_method_job, constants.API_LOCAL, data=data, notification_handler=got_error)

```

#### Remote Job example

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
    
## Configuration 

The library can be configured in some key attibutes as follows:

#### rabbitmq_url

The url of the rabbitmq server to be used.

#### max_retries

The no of maximum retries to be done for a retriable job before giving up.

#### async_timeout

The timeout for a remote call if the job is a remote call type.

#### eqc_sleep_duration

The duration of sleep for the retry worker inbetween every consuption cycle.

#### default_worker_count

The no of job workers to be spawned.

#### default_retry_consumer_count

No of retry consumer to be spawned

#### default_dl_consumer_count

No of dead letter consumer to be spawned

#### import_paths

semi color seperated list of import paths to be used by the library to search the modules 
for the job api passed for local job types.

#### pid_file_path

The file location of the pid file where the pid's of the workers are saved. 
DEFAULT: /var/tmp/easyjoblite.pid

#### workers_log_file_path

The log file location for the workers
DEFAULT: /var/tmp/easyjoblite.log

### max_worker_count 

The maximum worker count which can be spawned (TO BO implemented)

## Key Usage points

Here are some of the key points to be kept in 

## Limitations

Here are the basic limitations which need to be kept in mind while using the library:
1. No central manager to manage the lifecycle of the worker processes.
2. No signal handling in the workers to handle clean exit of the workers( e.g. complete the current job and then exit)
3. RabbitMQ is the only backend supported.
4. If no notification handler is provided the message in deal letter queue is lost.
5. Message ttl is not supported.

## Roadmap

The current limitation of the library is planed to be addressed in the future releases.
also below are some of the key features which are planed for future releases:
1. A central manager which owns the lifecycle of the workers and spawnes and kills workers as needed.

## Project history

This project has been inspired by the good parts of [Celery][1], [RabbitMQ][2]
and [Reddis Queue][3], and has been created as a lightweight alternative to the
heaviness of Celery or other AMQP-based queueing implementations.


[1]: http://www.celeryproject.org/
[2]: https://www.rabbitmq.com
[3]: http://python-rq.org/
