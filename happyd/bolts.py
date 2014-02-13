from happyd import logger
from happyd.connections import zk
from happyd.peers import PeerDiscovery


import random
import sys
import zerorpc


class Bolt(PeerDiscovery):
    def __init__(self, name):
        self.uid = random.getrandbits(32)
        self.name = name
        self.node = 'happydistribute/bolts/%d' % self.uid
        self.endpoint = 'tcp://0.0.0.0:4242'

        super(Bolt, self).__init__()

    def _register(self):
        logger.info('Registering "%s" with network' % self.name)

        zk.ensure_path(self.node)
        zk.set(self.node, '%s|%s' % (self.name, self.endpoint))

    def _unregister(self):
        logger.info('Removing "%s" from network' % self.name)

        zk.delete(self.node)

    def handle(self, data):
        raise NotImplementedError('Bolt handler is not implemented')


class BoltServer(zerorpc.Server):
    def __init__(self, methods=None, name=None, context=None, pool_size=None,
            heartbeat=5):

        self.methods_cls = methods

        super(BoltServer, self).__init__(methods, name, context, pool_size, heartbeat)

    def bind(self, endpoint, resolve=True):
        self.methods_cls.endpoint = endpoint
        super(BoltServer, self).bind(endpoint, resolve)

    def run(self):
        self.methods_cls._register()

        try:
            super(BoltServer, self).run()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self.methods_cls._unregister()

        zk.stop()
        zk.close()

        logger.info('Stopping server')

        super(BoltServer, self).stop()
        sys.exit(0)
