from happy_distributed.spouts import Spout
from happy_distributed.server import Server


class SimpleSpout(Spout):
    pass


server = Server(SimpleSpout('simpleSpout'))
server.run()
