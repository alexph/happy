from happy_distributed.components import BaseComponent
from happy_distributed.peers import PeerDiscovery


import zerorpc


class Spout(BaseComponent, PeerDiscovery):
    sub_child = 'spouts'

    def __init__(self, name):
        super(Spout, self).__init__(name)

        self._start_discovery()

    @zerorpc.stream
    def stream_data(self):
        return self._get_data()

    def _get_data(self):
        raise NotImplementedError('Spout %s _get_data method not implemented' % self.name)
