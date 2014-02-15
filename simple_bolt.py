from happy_distributed.bolts import Bolt
from happy_distributed.server import Server


class SimpleBolt(Bolt):
    connect_to = 'tcp://0.0.0.0:4242'

    def handle(self, data):
        print data

server = Server(SimpleBolt('simpleBolt'))
server.bind('tcp://0.0.0.0:4243')
server.run()
