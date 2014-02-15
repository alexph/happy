from happy_distributed import logger
from happy_distributed.connections import zk

import random
import socket


class BaseComponent(object):
    sub_child = None

    def __init__(self, name):
        if self.sub_child is None:
            raise NotImplementedError('Component sub_child attribute is not set ')

        self.uid = random.getrandbits(32)
        self.name = name
        self.node = 'happydistribute/%s/%d' % (self.sub_child, self.uid)
        self.endpoint = 'tcp://%s:4242' % socket.gethostbyname(socket.gethostname())

        logger.debug('Using uid %d' % self.uid)

    def _register(self):
        logger.info('Registering "%s" with nodes' % self.name)

        zk.ensure_path(self.node)
        zk.set(self.node, '%s|%s' % (self.name, self.endpoint))

    def _unregister(self):
        logger.info('Removing "%s" from nodes' % self.name)

        zk.delete(self.node)

