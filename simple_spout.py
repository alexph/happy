from happy_distributed.spouts import Spout
from happy_distributed.server import Server

import random

class SimpleSpout(Spout):
    def _get_data(self):
        while True:
            yield random.randint(0, 10)


server = Server(SimpleSpout('simpleSpout'))
server.bind('tcp://0.0.0.0:4242')
server.run()
