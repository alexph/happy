from happy_distributed import logger
from happy_distributed.threading import Workflow


import sys


class BaseConsumer(object):
    def handle(self, data):
        raise NotImplementedError('BaseConsumer handle method must be implemented')


class Topology(object):
    def __init__(self, name):
        self.name = name
        self.feed_name = None
        self.feed_method = None
        self.workflow = []

        logger.debug('new topology %s', name)

    def set_feed(self, name, method):
        self.feed_name = name
        self.feed_method = method

        logger.debug('set feed %s' % name)

    def set_consumer(self, name, method, grouping=[], pool=1):
        if not callable(method):
            if not isinstance(method, BaseConsumer):
                raise TypeError('Consumer must be callable or a BaseConsumer instance')

        if isinstance(method, BaseConsumer):
            method = method.handle

        self.workflow.append({
            'name': name,
            'method': method,
            'grouping': grouping,
            'pool': pool
        })

        logger.debug('set consumer %s %s' % (name, ', '.join(grouping)))

    def submit(self):
        wf = Workflow(self)

        try:
            wf.run()
        except KeyboardInterrupt:
            sys.exit(0)


