from happyd.bolts import Bolt, BoltServer


class SimpleBolt(Bolt):
    pass

server = BoltServer(SimpleBolt('simple'))

server.run()
