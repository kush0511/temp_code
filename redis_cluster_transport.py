from time import time

from queue import Empty
from kombu.utils.compat import _detect_environment
from kombu.utils.encoding import bytes_to_str
from kombu.utils.eventio import READ, ERR
from kombu.utils.json import loads
from kombu.log import get_logger
from kombu.utils.functional import accepts_argument
from kombu.utils.url import _parse_url
from kombu.transport.redis import (
    Channel as RedisChannel,
    MultiChannelPoller,
    Mutex,
    MutexHeld,
    QoS as RedisQoS,
    Transport as RedisTransport,
)
import redis
try:
    from redis.connection import (
        Connection,
        SSLConnection,
    )
    from redis.exceptions import MovedError
    import redis.cluster as rediscluster
except ImportError:
    rediscluster = None


logger = get_logger('ai_bus_commons.celery.redis_cluster_transport')
crit, warn = logger.critical, logger.warn


class QoS(RedisQoS):

    def restore_visible(self, start=0, num=10, interval=10):
        self._vrestore_count += 1
        if (self._vrestore_count - 1) % interval:
            return
        with self.channel.conn_or_acquire() as client:
            ceil = time() - self.visibility_timeout

            try:
                with Mutex(
                    client,
                    self.unacked_mutex_key,
                    self.unacked_mutex_expire,
                ):
                    env = _detect_environment()
                    if env == 'gevent':
                        ceil = time()

                    visible = client.zrevrangebyscore(
                        self.unacked_index_key,
                        ceil,
                        0,
                        start=num and start,
                        num=num,
                        withscores=True
                    )

                    for tag, score in visible or []:
                        self.restore_by_tag(tag, client)
            except MutexHeld:
                pass


class ClusterPoller(MultiChannelPoller):

    def __init__(self):
        self.conn_to_node_brpop = {}
        self.conn_to_node_listen = {}

        super().__init__()

    def _register(self, channel, client, conn, cmd):
        ident = (channel, client, conn, cmd)

        if ident in self._chan_to_sock:
            self._unregister(*ident)

        if conn._sock is None:
            conn.connect()

        sock = conn._sock
        self._fd_to_chan[sock.fileno()] = (channel, conn, cmd)
        self._chan_to_sock[ident] = sock
        self.poller.register(sock, self.eventflags)

    def _unregister(self, channel, client, conn, cmd):
        sock = self._chan_to_sock[(channel, client, conn, cmd)]
        self.poller.unregister(sock)

    def _register_BRPOP(self, channel):
        conns = self._get_conns_for_channel(channel)
        for conn in conns:
            ident = (channel, channel.client, conn, 'BRPOP')

            if (conn._sock is None or ident not in self._chan_to_sock):
                channel._in_poll = False
                self._register(*ident)

        if not channel._in_poll:  # send BRPOP
            channel._brpop_start()

    def _register_LISTEN(self, channel):
        conns = self._get_fanout_conns_for_channel(channel)

        for conn in conns:
            ident = (channel, channel.subclient, conn, 'LISTEN')

            if (conn._sock is None or ident not in self._chan_to_sock):
                channel._in_listen = False
                node = self.conn_to_node_listen[(conn.host, conn.port)]
                channel.subclient.set_pubsub_node(channel.client, node)
                channel.subclient.connection = conn
                self._register(*ident)

        if not channel._in_listen:
            channel._subscribe()  # send SUBSCRIBE

    def _get_conns_for_channel(self, channel):
        all_conn = []
        if self._chan_to_sock and self.conn_to_node_brpop:
            return [conn for _, _, conn, cmd in self._chan_to_sock if cmd == "BRPOP"]

        for key in channel.active_queues:
            node = channel.client.get_node_from_key(key)
            redis_node = channel.client.get_redis_connection(node)
            conn = redis_node.connection_pool.get_connection(key, 'NOOP')
            if (conn.host, conn.port) not in self.conn_to_node_brpop:
                all_conn.append(conn)
                self.conn_to_node_brpop[(conn.host, conn.port)] = node
        return all_conn

    def _get_fanout_conns_for_channel(self, channel):
        all_conn = []
        if self._chan_to_sock and self.conn_to_node_listen:
            return [conn for _, _, conn, cmd in self._chan_to_sock if cmd == "LISTEN"]

        for key in channel.active_fanout_queues:
            node = channel.client.get_node_from_key(key)
            channel.subclient.set_pubsub_node(channel.client, node)
            redis_node = channel.subclient.get_redis_connection()
            conn = redis_node.connection_pool.get_connection(key, 'NOOP')
            if (conn.host, conn.port) not in self.conn_to_node_listen:
                all_conn.append(conn)
                self.conn_to_node_listen[(conn.host, conn.port)] = node
        return all_conn

    def handle_event(self, fileno, event):
        if event & READ:
            return self.on_readable(fileno), self
        elif event & ERR:
            chan, conn, cmd = self._fd_to_chan[fileno]
            options = {'connection': conn}
            chan._poll_error(cmd, **options)

    def on_readable(self, fileno):
        try:
            chan, conn, cmd = self._fd_to_chan[fileno]
        except KeyError:
            return

        if chan.qos.can_consume():
            if cmd == "BRPOP":
                return chan.handlers[cmd](**{'conn': conn})
            else:
                return chan.handlers[cmd](**{'node': self.conn_to_node_listen[(conn.host, conn.port)]})


class Channel(RedisChannel):

    QoS = QoS
    connection_class = Connection if rediscluster else None
    connection_class_ssl = SSLConnection if rediscluster else None
    socket_keepalive = True

    namespace = 'default'
    keyprefix_queue = '/{namespace}/_kombu/binding%s'
    keyprefix_fanout = '/{namespace}/_kombu/fanout.'
    unacked_key = '/{namespace}/_kombu/unacked'
    unacked_index_key = '/{namespace}/_kombu/unacked_index'
    unacked_mutex_key = '/{namespace}/_kombu/unacked_mutex'

    min_priority = 0
    max_priority = 0
    priority_steps = [min_priority]

    from_transport_options = RedisChannel.from_transport_options + (
        'namespace',
        'keyprefix_queue',
        'keyprefix_fanout',
    )

    def __init__(self, conn, *args, **kwargs):
        options = conn.client.transport_options
        namespace = options.get('namespace', self.namespace)
        self.brpop_conn = set()
        self.listen_conn = set()
        keys = [
            'keyprefix_queue',
            'keyprefix_fanout',
            'unacked_key',
            'unacked_index_key',
            'unacked_mutex_key',
        ]

        for key in keys:
            value = options.get(key, getattr(self, key))
            options[key] = value.format(namespace=namespace)

        super().__init__(conn, *args, **kwargs)
        self.client.info()

    def _get_client(self):
        return rediscluster.RedisCluster

    def _create_client(self, asynchronous=False):
        params = self._connparams(asynchronous=asynchronous)
        params.pop('connection_class', None)
        if asynchronous:
            return self.Client(**params)
        return self.Client(**params)

    def _subscribe(self):
        keys = [self._get_subscribe_topic(queue)
                for queue in self.active_fanout_queues]
        if not keys:
            return
        c = self.subclient
        if keys:
            c.psubscribe(keys)
            self._in_listen = c.get_redis_connection().connection_pool.get_connection(keys[0], 'NOOP')

    def _receive(self, **options):
        ret = []
        node = options.pop('node')
        c = self.subclient
        c.set_pubsub_node(self.client, node)
        self.listen_conn.add(c.get_redis_connection())
        try:
            ret.append(self._receive_one(c))
        except Empty:
            pass
        while c.connection is not None and c.connection.can_read(timeout=0):
            ret.append(self._receive_one(c))
        return any(ret)

    def _brpop_start(self, timeout=1):
        queues = self._queue_cycle.consume(len(self.active_queues))
        if not queues:
            return

        self._in_poll = True
        timeout = timeout or 0
        node_to_keys = {}

        for key in queues:
            node = self.client.get_node_from_key(key)
            node_to_keys.setdefault(node.name, []).append(key)

        for chan, client, conn, cmd in self.connection.cycle._chan_to_sock:
            expected = (self, self.client, 'BRPOP')
            node_name = rediscluster.get_node_name(host=conn.host,
                                                   port=conn.port)
            keys = node_to_keys.get(node_name)
            if keys and (chan, client, cmd) == expected:
                for key in keys:
                    conn.send_command('BRPOP', key, timeout)

    def _brpop_read(self, **options):
        client = self.client

        try:
            conn = options.pop('conn')
            node = client.get_node(conn.host, conn.port)
            self.brpop_conn.add(conn)
            try:
                resp = node.redis_connection.parse_response(conn, 'BRPOP', **options)
            except self.connection_errors:
                conn.disconnect()
                raise
            except MovedError as e:
                # copied from rediscluster/client.py
                client.reinitialize_counter += 1
                if client._should_reinitialized():
                    client.nodes_manager.initialize()
                    # Reset the counter
                    client.reinitialize_counter = 0
                else:
                    self.nodes_manager.update_moved_exception(e)
                raise Empty()

            if resp:
                dest, item = resp
                dest = bytes_to_str(dest).rsplit(self.sep, 1)[0]
                self._queue_cycle.rotate(dest)
                self.connection._deliver(loads(bytes_to_str(item)), dest)
                return True
            else:
                raise Empty()
        finally:
            self._in_poll = False

    def _poll_error(self, type, **options):
        if type == 'LISTEN':
            self.subclient.parse_response()
        else:
            self.client.parse_response(options.pop('connection'), type)

    def _close_clients(self):
        # Close connections
        for conn in self.brpop_conn:
            try:
                conn.disconnect()
            except (KeyError, AttributeError, self.ResponseError):
                pass

        for conn in self.listen_conn:
            try:
                conn.disconnect()
            except (KeyError, AttributeError, self.ResponseError):
                pass

    def _connparams(self, asynchronous=False):
        conninfo = self.connection.client
        connparams = {
            'host': conninfo.hostname or '127.0.0.1',
            'port': conninfo.port or self.connection.default_port,
            'virtual_host': conninfo.virtual_host,
            'username': conninfo.userid,
            'password': conninfo.password,
            'max_connections': self.max_connections,
            'socket_timeout': self.socket_timeout,
            'socket_connect_timeout': self.socket_connect_timeout,
            'socket_keepalive': self.socket_keepalive,
            'socket_keepalive_options': self.socket_keepalive_options,
            'health_check_interval': self.health_check_interval,
            'retry_on_timeout': self.retry_on_timeout,
        }

        conn_class = self.connection_class

        # If the connection class does not support the `health_check_interval`
        # argument then remove it.
        if (
            hasattr(conn_class, '__init__')
                and not accepts_argument(conn_class.__init__, 'health_check_interval')):
            connparams.pop('health_check_interval')

        if conninfo.ssl:
            # Connection(ssl={}) must be a dict containing the keys:
            # 'ssl_cert_reqs', 'ssl_ca_certs', 'ssl_certfile', 'ssl_keyfile'
            try:
                connparams.update({'ssl': True})
                connparams.update(conninfo.ssl)
                connparams['connection_class'] = self.connection_class_ssl
            except TypeError:
                pass
        host = connparams['host']
        if '://' in host:
            scheme, _, _, username, password, path, query = _parse_url(host)
            if scheme == 'socket':
                connparams = self._filter_tcp_connparams(**connparams)
                connparams.update({
                    'connection_class': redis.UnixDomainSocketConnection,
                    'path': '/' + path}, **query)

                connparams.pop('socket_connect_timeout', None)
                connparams.pop('socket_keepalive', None)
                connparams.pop('socket_keepalive_options', None)
            connparams['username'] = username
            connparams['password'] = password

            connparams.pop('host', None)
            connparams.pop('port', None)

        channel = self
        connection_cls = (
            connparams.get('connection_class')
            or self.connection_class
        )

        if asynchronous:
            class Connection(connection_cls):
                def disconnect(self):
                    super().disconnect()
                    channel._on_connection_disconnect(self)
            connection_cls = Connection

        connparams['connection_class'] = connection_cls

        return connparams


class RedisClusterTransport(RedisTransport):

    Channel = Channel

    driver_type = 'cluster-redis'
    driver_name = driver_type

    def __init__(self, *args, **kwargs):
        if rediscluster is None:
            raise ImportError('dependency missing: redis-py')

        super().__init__(*args, **kwargs)
        self.cycle = ClusterPoller()

    def driver_version(self):
        return redis.__version__
