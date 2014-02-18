from happy_distributed import logger
from happy_distributed.hub import Topology, BaseConsumer

import gevent

def my_feed():
    with open('chekhov.txt') as f:
        for l in f.readlines():
            yield l
            gevent.sleep(0.1)


def split_words(data):
    for w in data.split(' '):
        if w.strip():
            yield w
            gevent.sleep(0)


class WordCount(BaseConsumer):
    count = 0

    def handle(self, data):
        self.count += 1
        gevent.sleep(0)
        return self.count


def printer(data):
    logger.info('Word count: %s', data)


t = Topology('myTopology')
t.set_feed('feed', my_feed)
t.set_consumer('split', split_words, pool=4)
t.set_consumer('count', WordCount(), pool=12)
t.set_consumer('printer', printer, pool=12)
t.submit()