from happyd import logger
from happyd.connections import zk

from msgpack.exceptions import ExtraData, UnpackValueError

import msgpack


class PeerDiscovery(object):
    def __init__(self):
        logger.debug('Starting peer discovery')
        self.peers = []
        self._watch_bolts()

    def _watch_bolts(self):
        def watch_bolt_peers(event):
            self._watch_bolts()

        bolts = zk.get_children('/happydistribute/bolts', watch=watch_bolt_peers)
        data = []

        for b in bolts:
            if not b in self.peers:
                zk_data = zk.get('happydistribute/bolts/%s' % b)
                b_data = zk_data[0].split('|')

                name = b_data[0]
                endpoint = b_data[1]

                logger.debug('Connected: %s %s' % (name, endpoint))
                logger.debug(zk_data[1])

                '''
                try:
                    b_msg = msgpack.loads(b_data[0])
                    logger.debug('Connected: %s %s' % b_msg['name'], b_msg['endpoint'])
                except ExtraData, exc:
                    logger.warning('msgpack: %s' % exc)
                except UnpackValueError, exc:
                    logger.warning('msgpack: %s' % exc)
                '''

        self.peers = data
