from confluent_kafka import Consumer, KafkaException, KafkaError
from collections import namedtuple
from random import randint
import sys
import logging
import time
import queue
from .parsing import parse_key, parse_value
from .helpers import clear_commits, ensure_dict_key
from jog import JogFormatter


this = sys.modules[__name__]
this.logger = None


def cleanup_conf(conf):
    new_conf = conf.copy()
    new_conf['bootstrap.servers'] = ','.join(conf['bootstrap_servers'])
    new_conf['group.id'] = conf['group_id']
    new_conf['auto.offset.reset'] =  conf['auto_offset_reset']
    new_conf['enable.auto.commit'] = False
    
    valid_options = (
        'builtin.features',
        'client.id',
        'metadata.broker.list',
        'bootstrap.servers',
        'message.max.bytes',
        'message.copy.max.bytes',
        'receive.message.max.bytes',
        'max.in.flight.requests.per.connection',
        'max.in.flight',
        'metadata.request.timeout.ms',
        'topic.metadata.refresh.interval.ms',
        'metadata.max.age.ms',
        'topic.metadata.refresh.fast.interval.ms',
        'topic.metadata.refresh.fast.cnt',
        'topic.metadata.refresh.sparse',
        'topic.blacklist',
        'debug',
        'socket.timeout.ms',
        'socket.blocking.max.ms',
        'socket.send.buffer.bytes',
        'socket.receive.buffer.bytes',
        'socket.keepalive.enable',
        'socket.nagle.disable',
        'socket.max.fails',
        'broker.address.ttl',
        'broker.address.family',
        'reconnect.backoff.jitter.ms',
        'reconnect.backoff.ms',
        'reconnect.backoff.max.ms',
        'statistics.interval.ms',
        'enabled_events',
        'error_cb',
        'throttle_cb',
        'stats_cb',
        'log_cb',
        'log_level',
        'log.queue',
        'log.thread.name',
        'log.connection.close',
        'background_event_cb',
        'socket_cb',
        'connect_cb',
        'closesocket_cb',
        'open_cb',
        'opaque',
        'default_topic_conf',
        'internal.termination.signal',
        'api.version.request',
        'api.version.request.timeout.ms',
        'api.version.fallback.ms',
        'broker.version.fallback',
        'security.protocol',
        'ssl.cipher.suites',
        'ssl.curves.list',
        'ssl.sigalgs.list',
        'ssl.key.location',
        'ssl.key.password',
        'ssl.certificate.location',
        'ssl.ca.location',
        'ssl.crl.location',
        'ssl.keystore.location',
        'ssl.keystore.password',
        'sasl.mechanisms',
        'sasl.mechanism',
        'sasl.kerberos.service.name',
        'sasl.kerberos.principal',
        'sasl.kerberos.kinit.cmd',
        'sasl.kerberos.keytab',
        'sasl.kerberos.min.time.before.relogin',
        'sasl.username',
        'sasl.password',
        'plugin.library.paths',
        'interceptors',
        'group.id',
        'partition.assignment.strategy',
        'session.timeout.ms',
        'heartbeat.interval.ms',
        'group.protocol.type',
        'coordinator.query.interval.ms',
        'max.poll.interval.ms',
        'enable.auto.commit',
        'auto.commit.interval.ms',
        'enable.auto.offset.store',
        'queued.min.messages',
        'queued.max.messages.kbytes',
        'fetch.wait.max.ms',
        'fetch.message.max.bytes',
        'max.partition.fetch.bytes',
        'fetch.max.bytes',
        'fetch.min.bytes',
        'fetch.error.backoff.ms',
        'offset.store.method',
        'consume_cb',
        'rebalance_cb',
        'offset_commit_cb',
        'enable.partition.eof',
        'check.crcs',
        'enable.idempotence',
        'enable.gapless.guarantee',
        'queue.buffering.max.messages',
        'queue.buffering.max.kbytes',
        'queue.buffering.max.ms',
        'linger.ms',
        'message.send.max.retries',
        'retries',
        'retry.backoff.ms',
        'queue.buffering.backpressure.threshold',
        'compression.codec',
        'compression.type',
        'batch.num.messages',
        'delivery.report.only.error',
        'dr_cb',
        'dr_msg_cb'
    )

    return {k: new_conf[k] for k in valid_options if k in new_conf}


def on_assignment(consumer, partitions):
    this.logger.info('Partition assignment: %r', partitions)


def _mp_consume(message_queue, report_inverval, json_logging, log_level, verbose, events, **consumer_options):
    conf = cleanup_conf(consumer_options)

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
        consumer = Consumer(conf, logger=this.logger)
        consumer.subscribe(['__consumer_offsets'], on_assign=on_assignment)

        start_time = time.time()
        current_report_interval = randint(1, report_inverval + 1)

        i = 0
        while True:
            # If we are asked to quit, do so - do not check to frequently
            if time.time() - start_time > current_report_interval and (events.exit.is_set() or events.stop.is_set()):
                consumer.close()
                break

            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    this.logger.debug('Reached end of [%d] at offset %d', message.partition(), message.offset())
                    continue
                else:
                    this.logger.error('poll() failed: %r', message.error())

            exporter_partition = message.partition()
            exporter_offset = message.offset()
            exporter_offsets = ensure_dict_key(exporter_offsets, exporter_partition, exporter_offset)
            exporter_offsets[exporter_partition] = exporter_offset
        
            if message.key() and message.value():
                key = parse_key(message.key())
                if key:
                    i += 1
                    value = parse_value(message.value())

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

                    except queue.Full:
                        this.logger.error('Queue is full, backing off')
                        current_report_interval *= 2
