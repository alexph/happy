from kazoo.client import KazooClient


zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

zk.ensure_path('happydistribute/bolts')
zk.ensure_path('happydistribute/spouts')
