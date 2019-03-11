from collections import namedtuple
from random import randint
import sys
import logging
import time
import queue
from kafka import KafkaConsumer
from .parsing import parse_key, parse_value
from .helpers import clear_commits, ensure_dict_key
from jog import JogFormatter


this = sys.modules[__name__]
this.logger = None


def _mp_consume(message_queue, report_inverval, json_logging, log_level, verbose, events, **consumer_options):
    log_handler = logging.StreamHandler()
    log_format = '[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s'
    formatter = JogFormatter(log_format) \
        if json_logging \
        else logging.Formatter(log_format)
    log_handler.setFormatter(formatter)

    log_level = getattr(logging, log_level)
    logging.basicConfig(
        handlers=[log_handler],
        level=logging.DEBUG if verbose else log_level
    )
    logging.captureWarnings(True)
    this.logger = logging.getLogger(__name__)

    offsets = {}
    commits = {}
    exporter_offsets = {}
    while not events.exit.is_set():
        # Wait till the controller indicates us to start consumption
        events.start.wait()

        this.logger.info('Initialising Consumer')
        consumer = KafkaConsumer('__consumer_offsets', **consumer_options)

        start_time = time.time()
        current_report_interval = randint(1, report_inverval + 1)

        while True:
            # If we are asked to quit, do so - do not check to frequently
            if time.time() - start_time > current_report_interval and (events.exit.is_set() or events.stop.is_set()):
                consumer.close()
                break

            i = 0
            for message in consumer:
                exporter_partition = message.partition
                exporter_offset = message.offset
                exporter_offsets = ensure_dict_key(exporter_offsets, exporter_partition, exporter_offset)
                exporter_offsets[exporter_partition] = exporter_offset
            
                if message.key and message.value:
                    key = parse_key(message.key)
                    if key:
                        i += 1
                        value = parse_value(message.value)

                        group = key[1]
                        topic = key[2]
                        partition = key[3]
                        offset = value[1]

                        offsets = ensure_dict_key(offsets, group, {})
                        offsets[group] = ensure_dict_key(offsets[group], topic, {})
                        offsets[group][topic] = ensure_dict_key(offsets[group][topic], partition, offset)
                        offsets[group][topic][partition] = offset

                        commits = ensure_dict_key(commits, group, {})
                        commits[group] = ensure_dict_key(commits[group], topic, {})
                        commits[group][topic] = ensure_dict_key(commits[group][topic], partition, 0)
                        commits[group][topic][partition] += 1

                        try:
                            if time.time() - start_time > current_report_interval:
                                this.logger.debug('Successfully processed %d/sec messages since last report', i / current_report_interval)
                                current_report_interval = randint(1, report_inverval + 1)
                                start_time = time.time()
                                message_queue.put((exporter_offsets, offsets, commits), timeout=report_inverval*2)
                                clear_commits(commits)
                                i = 0
                                if events.exit.is_set() or events.stop.is_set():
                                    consumer.close()
                                    break

                        except queue.Full:
                            this.logger.error('Queue is full, backing off')
                            current_report_interval *= current_report_interval
