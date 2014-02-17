from happy_distributed.hub import Topology, BaseConsumer

import gevent
import random


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
    print data


t = Topology('myTopology')
t.set_feed('feed', my_feed)
t.set_consumer('split', split_words)
t.set_consumer('count', WordCount())
t.set_consumer('printer', printer)
# t.set_consumer('consumer2', my_consumer)
# t.set_consumer('consumer3', my_consumer)
# t.set_consumer('consumer4', my_consumer)
# t.set_consumer('consumer5', my_consumer)
t.submit()