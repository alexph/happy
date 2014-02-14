from happy_distributed import logger
from happy_distributed.connections import zk
from happy_distributed.components import BaseComponent
from happy_distributed.peers import PeerDiscovery


import random
import sys
import zerorpc


class Bolt(BaseComponent, PeerDiscovery):
    sub_child = 'bolts'

    def __init__(self, name):
        super(Bolt, self).__init__(name)

        self._start_discovery()
