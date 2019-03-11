from collections import namedtuple
import logging
import queue
from multiprocessing import Process, Manager as MPManager
from .confluent_python_consumer import _mp_consume as _mp_consume_confluent_kafka
from .kafka_python_consumer import _mp_consume as _mp_consume_kafka_python


Events = namedtuple("Events", ["start", "stop", "exit"])


class MultiProcessConsumer:
    def __init__(self, use_confluent_kafka=False, num_procs=1, report_interval=5, json_logging=False, log_level='INFO', verbose=False, **consumer_options):

        # Variables for managing and controlling the data flow from
        # consumer child process to master
        manager = MPManager()
        self.queue = manager.Queue(1024)   # Child consumers dump messages into this
        self.events = Events(
            start = manager.Event(),        # Indicates the consumers to start fetch
            stop = manager.Event(),         # Indicates to stop fetching and pushing data till start is set back
            exit  = manager.Event()         # Requests the consumers to shutdown
        )
        self.report_interval = report_interval
        self.num_procs = num_procs
        self.consumer_options = consumer_options
        self.json_logging = json_logging
        self.verbose = verbose
        self.log_level = log_level

        self.procs = []
        for proc in range(self.num_procs):
            args = (self.queue, self.report_interval, self.json_logging, self.log_level, self.verbose, self.events)
            proc = Process(target=_mp_consume_confluent_kafka if use_confluent_kafka else _mp_consume_kafka_python, args=args, kwargs=consumer_options)
            proc.daemon = True
            proc.start()
            self.procs.append(proc)


    def stop(self):
        self.events.exit.set()
        self.events.stop.set()
        self.events.start.set()

        for proc in self.procs:
            proc.join()
            proc.terminate()


    def __iter__(self):
        # first check if any of the child processes died
        for proc in self.procs:
            if not proc.is_alive():
                raise Exception("Child process with PID %d died with %d exitcode" % (proc.pid, proc.exitcode))

        while True:
            self.events.stop.clear()
            self.events.start.set()

            try:
                # We will block for a report_interval based time so that the consumers get
                # a chance to run and put some messages in the queue
                message = self.queue.get(block=True, timeout=self.report_interval*2)
            except queue.Empty:
                break

            yield message

        self.events.stop.set()
        self.events.start.clear()
