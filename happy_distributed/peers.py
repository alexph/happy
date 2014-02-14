from happy_distributed import logger
from happy_distributed.connections import zk


class PeerDiscovery(object):
    def _start_discovery(self):
        logger.debug('Starting peer discovery')
        self.peers = []
        self._set_watcher('bolts')
        self._set_watcher('spouts')

    def _set_watcher(self, sub_child):
        def watch_peers(event):
            self._set_watcher(sub_child)

        children = zk.get_children('/happydistribute/%s' % sub_child, watch=watch_peers)
        data = []

        for c in children:
            zk_data = zk.get('/happydistribute/%s/%s' % (sub_child, c))

            if zk_data[0]:
                c_data = zk_data[0].split('|')

                if isinstance(c_data, (tuple, list)):
                    try:
                        name = c_data[0]
                        endpoint = c_data[1]

                        logger.debug('%s presence: %s %s' % (sub_child, name, endpoint))
                    except IndexError, exc:
                        logger.warn('%s: %s' % (exc, zk_data[0]))

        self.peers = data
