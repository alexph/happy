from happy_distributed import logger
from happy_distributed.threading import Workflow

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

    def set_consumer(self, name, method, grouping=[]):
        self.workflow.append({
            'name': name,
            'method': method,
            'grouping': grouping
        })

        logger.debug('set consumer %s %s' % (name, ', '.join(grouping)))

    def submit(self):
        wf = Workflow(self)
        wf.run()

