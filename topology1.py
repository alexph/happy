from happy_distributed.hub import Topology

import gevent
import random


def my_feed():
    yield random.randint(1, 100)
    gevent.sleep(1)


def my_consumer(data):
    return data


t = Topology('myTopology')
t.set_feed('feed', my_feed)
t.set_consumer('consumer1', my_consumer)
t.set_consumer('consumer2', my_consumer)
t.set_consumer('consumer3', my_consumer)
t.set_consumer('consumer4', my_consumer)
t.set_consumer('consumer5', my_consumer)
t.submit()