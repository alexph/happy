from happy_distributed import logger
from happy_distributed.connections import zk


import random


class BaseComponent(object):
    sub_child = None
    handler_impl_exc = 'Component handler method is not implemented'

    def __init__(self, name):
        if self.sub_child is None:
            raise NotImplementedError('Component sub_child attribute is not set ')

        self.uid = random.getrandbits(32)
        self.name = name
        self.node = 'happydistribute/%s/%d' % (self.sub_child, self.uid)
        self.endpoint = 'tcp://0.0.0.0:4242'

    def _register(self):
        logger.info('Registering "%s" with network' % self.name)

        zk.ensure_path(self.node)
        zk.set(self.node, '%s|%s' % (self.name, self.endpoint))

    def _unregister(self):
        logger.info('Removing "%s" from network' % self.name)

        zk.delete(self.node)

    def handle(self, data):
        raise NotImplementedError(self.handler_impl_exc)