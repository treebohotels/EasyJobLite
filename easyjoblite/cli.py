# -*- coding: utf-8 -*-
"""
work-queue consumer for b2b booking-exchange
"""

import logging

import click
import constants
import orchestrator
import os
import state
import utils

logging.basicConfig()


@click.group()
def main():
    """easy job lite command line tool"""
    pass


@main.command()
@click.argument("type")
@click.option("-u", "--url",
              help="rabbitmq connection url, e.g.: amqp://b2badmin:b2badmin@localhost/b2b",
              default=constants.DEFAULT_RMQ_URL)
@click.option("--import_paths",
              help="default import path for the modules",
              default=constants.DEFAULT_IMPORT_PATHS)
@click.option("--max_retries",
              help="max no of retries for a job",
              default=constants.DEFAULT_MAX_JOB_RETRIES)
@click.option("--asyc_timeout",
              help="async timeout for remote calls",
              default=constants.DEFAULT_ASYNC_TIMEOUT)
@click.option("--eqc_sleep_duration",
              help="the sleep duration for the retry queue",
              default=constants.DEFAULT_ERROR_Q_CON_SLEEP_DURATION)
@click.option("--workers_log_file_path",
              help="the log file for all the workers",
              default=constants.DEFAULT_LOG_FILE_PATH)
@click.option("--dead_message_log_file",
              help="the default dead letter log file",
              default=constants.DEFAULT_DL_LOG_FILE)
@click.option("--config_file",
              help="the config file path",
              default=constants.DEFAULT_CONFIG_FILE)
def start(type, url, import_paths, max_retries, asyc_timeout, eqc_sleep_duration, workers_log_file_path,
          dead_message_log_file, config_file):
    """command to start a worker"""
    # todo: get rabbitmq config params from command line (e.g. user, passwd, host separately)
    logger = logging.getLogger("easyjobcli:start")

    orst = orchestrator.Orchestrator(rabbitmq_url=url,
                                     async_timeout=int(asyc_timeout),
                                     import_paths=import_paths,
                                     max_retries=int(max_retries),
                                     eqc_sleep_duration=int(eqc_sleep_duration),
                                     workers_log_file_path=workers_log_file_path,
                                     dead_message_log_file=dead_message_log_file,
                                     config_file=config_file
                                     )

    pid = os.getpid()

    orst.update_consumer_pid(type, pid)

    logger.info("started worker of type {} with pid {}".format(type, pid))

    orst.start_consumer(type)


@main.command()
@click.argument('type')
def stop(type):
    """command to stop the processes"""
    logger = logging.getLogger("easyjobcli:stop")
    service_state = state.ServiceState()
    worker_type_list = [constants.WORK_QUEUE, constants.RETRY_QUEUE, constants.DEAD_LETTER_QUEUE]

    if type in worker_type_list:
        utils.kill_workers(service_state, type)
        logger.info("Done stoping all the workers of type {}".format(type))
    elif type == constants.STOP_TYPE_ALL:
        for local_type in worker_type_list:
            utils.kill_workers(service_state, local_type)
        logger.info("Done stoping all the workers ")
    else:
        click.echo("Invalid type send to stop statement.")


@main.command()
def info():
    """show the state of the service"""
    service_state = state.ServiceState()

    click.echo("JOB   WORKER PIDS: " + str(service_state.get_pid_list(constants.WORK_QUEUE)))
    click.echo("RETRY WORKER PIDS: " + str(service_state.get_pid_list(constants.RETRY_QUEUE)))
    click.echo("DLQ   WORKER PIDS: " + str(service_state.get_pid_list(constants.DEAD_LETTER_QUEUE)))
