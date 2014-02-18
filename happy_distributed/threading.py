from happy_distributed import logger

from gevent.queue import Queue
from gevent.pool import Pool

import gevent


def feed_loop(call, tasks):
    while True:
        for task in call():
            if task is not None:
                tasks.put(task)
        gevent.sleep(0)


def worker(call, name, input, output, pool):
    subs = Pool(pool)

    while True:
        if not input.empty():
            def run():
                task = input.get()
                logger.debug('Worker %s got task %s' % (name, task))

                if output is not None:
                    r = call(task)
                    try:
                        for x in r:
                            output.put(x)
                    except TypeError:
                        output.put(r)
                else:
                    call(task)

            subs.spawn(run)

        gevent.sleep(0)


class Workflow(object):
    def __init__(self, topology):
        logger.debug('Workflow: %s' % topology.name)

        self.topology = topology
        self.workflow = self.topology.workflow

        self.queues = []

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

            threads.append(gevent.spawn(worker, w['method'], w['name'],
                                        input, output, w['pool']))

        gevent.joinall(threads)
