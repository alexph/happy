from happy_distributed import logger

from gevent.queue import Queue


import gevent
import types


topologies = list()


def feed_loop(call, tasks):
    while True:
        for task in call():
            if task is not None:
                tasks.put(task)
        gevent.sleep(0)


def worker(call, name, input, output):
    while True:
        if not input.empty():
            task = input.get()
            logger.debug('Worker %s got task %s' % (name, task))

            if isinstance(call, types.GeneratorType):
                for x in call(task):
                    if output is not None:
                        output.put(x)
            else:
                x = call(task)

                if output is not None:
                    output.put(x)

        gevent.sleep(0)


class Workflow(object):
    def __init__(self, topology):
        logger.debug('Workflow: %s' % topology.name)
        self.topology = topology
        self.flow = []

        self.queues = []

        self.workflow = self.topology.workflow

        order = 0

        for w in self.workflow:
            logger.debug('Pool %s' % w['name'])
            if order > 0:
                self.queues.append(Queue())
            w['input_index'] = order - 1
            w['output_index'] = order

            order +=1

        print self.workflow

    def run(self):
        tasks = Queue()
        feed = gevent.spawn(feed_loop, self.topology.feed_method, tasks)

        threads = [feed]

        for w in self.workflow:
            if w['input_index'] < 0:
                input = tasks
            else:
                input = self.queues[w['input_index']]

            try:
                output = self.queues[w['output_index']]
            except IndexError:
                output = None

            threads.append(gevent.spawn(worker, w['method'], w['name'], input, output))

        gevent.joinall(threads)
