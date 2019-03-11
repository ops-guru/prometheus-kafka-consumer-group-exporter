import argparse
import javaproperties
import logging
import signal
import sys
import random
import string

from jog import JogFormatter
from kafka import KafkaConsumer
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY

from . import scheduler, collectors
from .fetch_jobs import setup_fetch_jobs
from .parsing import parse_key, parse_value
from .mp_consume import MultiProcessConsumer
from .helpers import ensure_dict_key

from pprint import pprint

def shutdown():
    logging.info('Shutting down')
    sys.exit(1)


def signal_handler(signum, frame):
    shutdown()


def merge_commits(target, dict):
    for group, group_value in dict.items():
        if group not in target:
            target = ensure_dict_key(target, group, group_value)
        else:
            for topic, topic_value in group_value.items():
                if topic not in target[group]:
                    target[group] = ensure_dict_key(target[group], topic, topic_value)
                else:
                    for partition, partition_value in topic_value.items():
                        if partition not in target[group][topic]:
                            target[group][topic] = ensure_dict_key(target[group][topic], partition, partition_value)
                        else:
                            target[group][topic][partition] += partition_value

    return target


def merge_offsets(target, dict):
    for group, group_value in dict.items():
        if group not in target:
            target = ensure_dict_key(target, group, group_value)
        else:
            for topic, topic_value in group_value.items():
                if topic not in target[group]:
                    target[group] = ensure_dict_key(target[group], topic, topic_value)
                else:
                    for partition, partition_value in topic_value.items():
                        if partition not in target[group][topic]:
                            target[group][topic] = ensure_dict_key(target[group][topic], partition, partition_value)
                        else:
                            target[group][topic][partition] = partition_value

    return target


def merge_exporter_offsets(target, dict):
    for partition, partition_value in dict.items():
        if partition not in target:
            target = ensure_dict_key(target, partition, partition_value)
        elif partition_value > target[partition]:
            target[partition] = partition_value

    return target


def id_generator(size=24, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def cleanup_conf(conf):
    valid_options = (
        'bootstrap_servers',
        'client_id',
        'group_id',
        'key_deserializer',
        'value_deserializer',
        'fetch_max_wait_ms',
        'fetch_min_bytes',
        'fetch_max_bytes',
        'max_partition_fetch_bytes',
        'request_timeout_ms',
        'retry_backoff_ms',
        'reconnect_backoff_ms',
        'reconnect_backoff_max_ms',
        'max_in_flight_requests_per_connection',
        'auto_offset_reset',
        'enable_auto_commit',
        'auto_commit_interval_ms',
        'default_offset_commit_callback',
        'check_crcs',
        'metadata_max_age_ms',
        'partition_assignment_strategy',
        'max_poll_records',
        'max_poll_interval_ms',
        'session_timeout_ms',
        'heartbeat_interval_ms',
        'receive_buffer_bytes',
        'send_buffer_bytes',
        'socket_options',
        'sock_chunk_bytes',
        'sock_chunk_buffer_count',
        'consumer_timeout_ms',
        'skip_double_compressed_messages',
        'security_protocol',
        'ssl_context',
        'ssl_check_hostname',
        'ssl_cafile',
        'ssl_certfile',
        'ssl_keyfile',
        'ssl_crlfile',
        'ssl_password',
        'api_version',
        'api_version_auto_timeout_ms',
        'connections_max_idle_ms',
        'metric_reporters',
        'metrics_num_samples',
        'metrics_sample_window_ms',
        'metric_group_prefix',
        'selector',
        'exclude_internal_topics',
        'sasl_mechanism',
        'sasl_plain_username',
        'sasl_plain_password',
        'sasl_kerberos_service_name',
        'sasl_kerberos_domain_name'
    )

    newconf = {k: conf[k] for k in valid_options if k in conf}

    return newconf


def main():
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(
        description='Export Kafka consumer offsets to Prometheus.')
    parser.add_argument(
        '-b', '--bootstrap-brokers',
        help='Addresses of brokers in a Kafka cluster to talk to.' +
        ' Brokers should be separated by commas e.g. broker1,broker2.' +
        ' Ports can be provided if non-standard (9092) e.g. brokers1:9999.' +
        ' (default: localhost)')
    parser.add_argument(
        '-p', '--port', type=int, default=9208,
        help='Port to serve the metrics endpoint on. (default: 9208)')
    parser.add_argument(
        '-c', '--consumers', type=int, default=1,
        help='Number of Kakfa consumers to use (parallelism)'
    )
    parser.add_argument(
        '--use-confluent-kafka', action='store_true',
        help='Use confluent_kafka rather than kafka-python for consumption'
    )
    parser.add_argument(
        '-s', '--from-start', action='store_true',
        help='Start from the beginning of the `__consumer_offsets` topic.')
    parser.add_argument(
        '--topic-interval', type=float, default=30.0,
        help='How often to refresh topic information, in seconds. (default: 30)')
    parser.add_argument(
        '--high-water-interval', type=float, default=10.0,
        help='How often to refresh high-water information, in seconds. (default: 10)')
    parser.add_argument(
        '--low-water-interval', type=float, default=10.0,
        help='How often to refresh low-water information, in seconds. (default: 10)')
    parser.add_argument(
        '--consumer-config', action='append', default=[],
        help='Provide additional Kafka consumer config as a consumer.properties file. Multiple files will be merged, later files having precedence.')
    parser.add_argument(
        '-j', '--json-logging', action='store_true',
        help='Turn on json logging.')
    parser.add_argument(
        '--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='detail level to log. (default: INFO)')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='turn on verbose (DEBUG) logging. Overrides --log-level.')
    args = parser.parse_args()

    log_handler = logging.StreamHandler()
    log_format = '[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s'
    formatter = JogFormatter(log_format) \
        if args.json_logging \
        else logging.Formatter(log_format)
    log_handler.setFormatter(formatter)

    log_level = getattr(logging, args.log_level)
    logging.basicConfig(
        handlers=[log_handler],
        level=logging.DEBUG if args.verbose else log_level
    )
    logging.captureWarnings(True)

    port = args.port

    consumer_config = {
        'auto_offset_reset': 'latest',
        'group_id': None,
        'consumer_timeout_ms': 500
    }

    # the same config is used both for kafka-python and confluent_kafka
    # most important properties have the same names (except _ being used instead of .)
    # one difference is that in case of single consumer kafka-python requires group_id not to be set
    # while confluent_kafka always requires to have group_id
    if not args.use_confluent_kafka:
        if args.consumers > 1:
            consumer_config['group_id'] = 'prometheus-kafka-consumer-exporter-' + id_generator()
            consumer_config['enable_auto_commit'] = False
    else:
        consumer_config['group_id'] = 'prometheus-kafka-consumer-exporter-' + id_generator()
   
    for filename in args.consumer_config:
        with open(filename) as f:
            raw_config = javaproperties.load(f)
            converted_config = {k: int(v) if v.isdigit() else True if v == 'True' else False if v == 'False' else v for k, v in raw_config.items()}
            consumer_config.update(converted_config)

    if not 'bootstrap_servers' in consumer_config:
        consumer_config['bootstrap_servers'] = 'localhost'
        logging.info('bootstrap_servers not specified - using localhost')

    if args.bootstrap_brokers:
        consumer_config['bootstrap_servers'] = args.bootstrap_brokers.split(',')

    if args.from_start:
        consumer_config['auto_offset_reset'] = 'earliest'

    # retain only settings relevant for kafka-python
    kafka_python_consumer_config = cleanup_conf(consumer_config)

    consumer = KafkaConsumer(
        **kafka_python_consumer_config
    )
    client = consumer._client

    topic_interval = args.topic_interval
    high_water_interval = args.high_water_interval
    low_water_interval = args.low_water_interval

    logging.info('Starting server...')
    start_http_server(port)
    logging.info('Server started on port %s', port)

    REGISTRY.register(collectors.HighwaterCollector())
    REGISTRY.register(collectors.LowwaterCollector())
    REGISTRY.register(collectors.ConsumerOffsetCollector())
    REGISTRY.register(collectors.ConsumerLagCollector())
    REGISTRY.register(collectors.ConsumerLeadCollector())
    REGISTRY.register(collectors.ConsumerCommitsCollector())
    REGISTRY.register(collectors.ExporterOffsetCollector())
    REGISTRY.register(collectors.ExporterLagCollector())
    REGISTRY.register(collectors.ExporterLeadCollector())

    scheduled_jobs = setup_fetch_jobs(topic_interval, high_water_interval, low_water_interval, client)

    mpc = MultiProcessConsumer(args.use_confluent_kafka, args.consumers, 5, args.json_logging, args.log_level, args.verbose, **consumer_config)
    try:
        while True:
            for item in mpc:

                offsets = collectors.get_offsets()
                commits = collectors.get_commits()
                exporter_offsets = collectors.get_exporter_offsets()

                exporter_offsets = merge_exporter_offsets(exporter_offsets, item[0])
                offsets = merge_offsets(offsets, item[1])
                commits = merge_offsets(commits, item[2])

                collectors.set_exporter_offsets(exporter_offsets)
                collectors.set_offsets(offsets)
                collectors.set_commits(commits)

                # Check if we need to run any scheduled jobs
                # each message.
                scheduled_jobs = scheduler.run_scheduled_jobs(scheduled_jobs)

            # Also check if we need to run any scheduled jobs
            # each time the consumer times out, in case there
            # aren't any messages to consume.
            scheduled_jobs = scheduler.run_scheduled_jobs(scheduled_jobs)

    except KeyboardInterrupt:
        pass

    mpc.stop()
    shutdown()