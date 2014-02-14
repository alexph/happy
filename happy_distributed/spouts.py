from happy_distributed.components import BaseComponent
from happy_distributed.peers import PeerDiscovery


class Spout(BaseComponent, PeerDiscovery):
    sub_child = 'spouts'

    def __init__(self, name):
        super(Spout, self).__init__(name)

        self._start_discovery()