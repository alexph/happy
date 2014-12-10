import functools
import logging
import Queue
import signal
import sys
import threading
import time


logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class ProducerStreamEnd(Exception):
    pass


class Store(object):
    def set(name, value):
        raise NotImplementedError()

    def get(name):
        raise NotImplementedError()


class MemoryStore(Store):
    store = {}

    def set(name, value):
        MemoryStore.store[name] = value

    def get(name):
        return MemoryStore.store.get(name)


class HappyContext(object):
    def __init__(self):
        self.store = MemoryStore()
        self.producers = {}
        self.consumers = {}

    def add_producer(self, name, spec):
        self.producers[name] = spec

    def add_consumer(self, name, spec):
        self.consumers[name] = spec

    def get_queue(self, name):
        return self.producers[name].queue


HAPPY_CONTEXT = HappyContext()


class Producer(threading.Thread):
    def __init__(self, spec):
        self.state = threading.Event()
        self.queue = Queue.Queue()
        self.spec = spec
        self.signal = True
        self._filter = lambda x: x
        self._map = lambda x: x

        super(Producer, self).__init__(target=self.worker)

    def worker(self):
        self.state.set()

        while self.signal:
            try:
                for x in self.spec():
                    if self._filter(x):
                        logger.debug('%s: %s' % (self.spec.__name__, x))
                        self.queue.put(self._map(x))

                    time.sleep(0.0001)
            except ProducerStreamEnd, exc:
                logger.info(exc)
                break
            time.sleep(0.1)

        self.state.clear()

    def filter(self, functor):
        self._filter = functor

    def map(self, functor):
        self._map = functor

    def merge(self):
        pass

    def left_join(self):
        pass

    def join(self):
        pass


class Consumer(threading.Thread):
    def __init__(self, producer, queue, spec):
        self.producer = producer
        self.queue = queue
        self.spec = spec
        self.signal = True

        #
        # Behaviour
        self._sum_key_store = False
        self._write_to = False

        super(Consumer, self).__init__(target=self.worker)

    def worker(self):
        while self.signal:
            if not self.queue.empty():
                item = self.queue.get()

                logger.debug('%s: %s' % (self.spec.__name__, item))

                if self._sum_key_store:
                    if hasattr(item, '__getitem__'):
                        logger.debug(item.keys())

                self.spec(item)
                self.queue.task_done()

            time.sleep(0.0001)

    def sum_keys(self, store):
        self._sum_key_store = store

    def collect(self, store):
        pass

    def write(self, output):
        self._write_to = output


def producer(func=None, **config):
    def decorator(f):
        producer = Producer(f)
        HAPPY_CONTEXT.add_producer(f.__name__, producer)

        # @functools.wraps(f)
        # def wrapper(*args, **kwargs):
        #     return producer(*args, **kwargs)
            
        # return wrapper
        return producer

    if func:
        return decorator(func)

    return decorator


def consumer(producer):
    def decorator(f):
        queue = HAPPY_CONTEXT.get_queue(producer.spec.__name__)
        consumer = Consumer(producer, queue, f)
        HAPPY_CONTEXT.add_consumer(f.__name__, consumer)

        # @functools.wraps(f)
        # def wrapper(*args, **kwargs):
        #     return consumer(*args, **kwargs)

        return consumer
    return decorator


class PipelineController(threading.Thread):
    def __init__(self, threads):
        logger.info('%s Starting' % self.__class__.__name__)

        self.threads = threads

        super(PipelineController, self).__init__(target=self.worker)

    def worker(self):
        while True:
            for x in self.threads:
                print x.__name__, x.signal
            time.sleep(1)


class HappyApp(object):
    def __init__(self):
        self.threads = []
        self.controller = None

    def start(self):
        for x in self.threads:
            x.start()
        self.controller.start()

    def run(self):
        def signal_handler(signal, frame):
            print('Warm Shutdown.')
            self.stop()
            sys.exit(0)
        signal.signal(signal.SIGINT, signal_handler)

        for x in HAPPY_CONTEXT.consumers.values():
            self.threads.append(x)

        for x in HAPPY_CONTEXT.producers.values():
            self.threads.append(x)

        self.controller = PipelineController(self.threads)

        self.start()
        signal.pause()

    def stop(self):
        self.controller.stop()
        
        for x in self.threads:
            x.signal = False

