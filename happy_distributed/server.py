from happy_distributed import logger
from happy_distributed.connections import zk

import sys
import time
import zerorpc


class Server(zerorpc.Server):
    """
    Wrapper around zerorpc server that accepts a Bolt instance
    and handles some extra registration and config.
    """
    def __init__(self, methods=None, name=None, context=None, pool_size=None,
            heartbeat=5):

        self.methods_cls = methods

        super(Server, self).__init__(methods, name, context, pool_size, heartbeat)

    def bind(self, endpoint, resolve=True):
        #
        # Is there a better way to get our endpoint?
        self.methods_cls.endpoint = endpoint

        super(Server, self).bind(endpoint, resolve)

    def run(self):
        self.methods_cls._register()

        try:
            super(Server, self).run()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self.methods_cls._unregister()

        if hasattr(self.methods_cls, '_client'):
            self.methods_cls._client.close()

        zk.stop()
        zk.close()

        logger.info('Stopping server')

        super(Server, self).stop()

        time.sleep(1)

        sys.exit(0)