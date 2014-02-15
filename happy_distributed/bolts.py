from happy_distributed.components import BaseComponent
from happy_distributed.peers import PeerDiscovery

import time
import zerorpc


class Bolt(BaseComponent, PeerDiscovery):
    sub_child = 'bolts'
    connect_to = None
    connected = False
    wait_time = 1

    def __init__(self, name):
        if not self.connect_to:
            raise Exception('I don\'t know what to listen to')

        super(Bolt, self).__init__(name)

        self._start_discovery()
        self._start_client()

    def _start_client(self):
        self._client = zerorpc.Client(self.connect_to)

        self._stream()

    def _stream(self):
        for x in self._client.stream_data():
            self.handle(x)

        time.sleep(self.wait_time)

        #
        # Next group of data
        self._stream()

    def handle(self, data):
        raise NotImplementedError('Bolt handler method is not implemented')
