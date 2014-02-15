from happy_distributed import logger
from happy_distributed.connections import zk


class PeerDiscovery(object):
    def _start_discovery(self):
        logger.debug('Starting peer discovery')
        self.peers = []
        self.peer_index = {}
        self._set_watcher('bolts')
        self._set_watcher('spouts')

    def _set_watcher(self, sub_child):
        def watch_peers(event):
            #
            # Reset watcher
            self._set_watcher(sub_child)

        children = zk.get_children('/happydistribute/%s' % sub_child, watch=watch_peers)

        #
        # New peers for sorting
        data = []

        for c in children:
            zk_data = zk.get('/happydistribute/%s/%s' % (sub_child, c))

            if zk_data[0]:
                c_data = zk_data[0].split('|')

                if isinstance(c_data, (tuple, list)):
                    try:
                        name = c_data[0]
                        endpoint = c_data[1]

                        data.append({
                            'uid': c,
                            'type': sub_child,
                            'name': name,
                            'endpoint': endpoint
                        })
                    except IndexError, exc:
                        logger.warn('%s: %s' % (exc, zk_data[0]))

        #
        # Reset peers name => data index
        self.peer_index = {}

        #
        # Updated peers list to set after comparison
        peers_update = []

        for d in data:
            peers_update.append(d['uid'])

            if d['uid'] not in self.peers:
                if sub_child == 'bolts':
                    self._on_presence_bolt(d['uid'], d['name'], d['endpoint'])
                elif sub_child == 'spouts':
                    self._on_presence_spout(d['uid'], d['name'], d['endpoint'])

            self.peer_index[d['name']] = d

        self.peers = peers_update

    def _on_presence_bolt(self, uid, name, endpoint):
        logger.debug('Unhandled bolt presence: %s, %s, %s' % (uid, name, endpoint))

    def _on_presence_spout(self, uid, name, endpoint):
        logger.debug('Unhandled spout presence: %s, %s, %s' % (uid, name, endpoint))
