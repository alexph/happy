from happy_distributed.bolts import Bolt
from happy_distributed.server import Server


class SimpleBolt(Bolt):
    pass

server = Server(SimpleBolt('simpleBolt'))

server.run()
