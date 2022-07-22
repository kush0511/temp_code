# -*- coding: utf-8 -*-
"""
    celery.backends.rediscluster
    ~~~~~~~~~~~~~~~~~~~~~
    Redis cluster result store backend.
    CELERY_REDIS_CLUSTER_BACKEND_SETTINGS = {
        startup_nodes: [{"host": "127.0.0.1", "port": "7000"}]
    }
"""
from __future__ import absolute_import

from functools import partial

from kombu.utils import cached_property, retry_over_time
import time
from contextlib import contextmanager
from celery import states
from celery._state import task_join_will_block
from celery.canvas import maybe_signature
from celery.exceptions import ChordError, ImproperlyConfigured
from celery.utils.serialization import strtobool
from celery.utils.log import get_logger
from celery.utils.time import humanize_seconds
from kombu.utils.url import _parse_url
from celery.utils.functional import dictfilter
from ssl import CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED
from urllib.parse import unquote
from redis import connection

from celery.backends.base import KeyValueStoreBackend
from celery.backends.asynchronous import BaseResultConsumer

# try:
from redis.cluster import RedisCluster

# from kombu.transport.redis import get_redis_error_classes
# except ImportError:                 # pragma: no cover
#    RedisCluster = None                    # noqa
#    ConnectionError = None          # noqa
get_redis_error_classes = None  # noqa

__all__ = ['RedisClusterBackend']

E_REDIS_MISSING = """\
You need to install the redis-py-cluster library in order to use \
the Redis result store backend."""

W_REDIS_SSL_CERT_OPTIONAL = """
Setting ssl_cert_reqs=CERT_OPTIONAL when connecting to redis means that \
celery might not validate the identity of the redis broker when connecting. \
This leaves you vulnerable to man in the middle attacks.
"""

W_REDIS_SSL_CERT_NONE = """
Setting ssl_cert_reqs=CERT_NONE when connecting to redis means that celery \
will not validate the identity of the redis broker when connecting. This \
leaves you vulnerable to man in the middle attacks.
"""

E_REDIS_SSL_PARAMS_AND_SCHEME_MISMATCH = """
SSL connection parameters have been provided but the specified URL scheme \
is rediss:// or cluster-redis://. A Redis SSL connection URL should use the scheme rediss:// or cluster-rediss://.
"""

E_REDIS_SSL_CERT_REQS_MISSING_INVALID = """
A rediss:// or cluster-rediss:// URL must have parameter ssl_cert_reqs and this must be set to \
CERT_REQUIRED, CERT_OPTIONAL, or CERT_NONE
"""

E_LOST = 'Connection to Redis lost: Retry (%s/%s) %s.'


E_RETRY_LIMIT_EXCEEDED = """
Retry limit exceeded while trying to reconnect to the Celery redis result \
store backend. The Celery application must be restarted.
"""


logger = get_logger(__name__)
error = logger.error


class ResultConsumer(BaseResultConsumer):
    _pubsub = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._get_key_for_task = self.backend.get_key_for_task
        self._decode_result = self.backend.decode_result
        self._ensure = self.backend.ensure
        self._connection_errors = self.backend.connection_errors
        self.subscribed_to = set()

    def on_after_fork(self):
        try:
            self.backend.client.connection_pool.reset()
            if self._pubsub is not None:
                self._pubsub.close()
        except KeyError as e:
            logger.warning(str(e))
        super().on_after_fork()

    def _reconnect_pubsub(self):
        self._pubsub = None
        self.backend.client.connection_pool.reset()
        # task state might have changed when the connection was down so we
        # retrieve meta for all subscribed tasks before going into pubsub mode
        if self.subscribed_to:
            metas = self.backend.client.mget(self.subscribed_to)
            metas = [meta for meta in metas if meta]
            for meta in metas:
                self.on_state_change(self._decode_result(meta), None)
        self._pubsub = self.backend.client.pubsub(
            ignore_subscribe_messages=True,
        )
        # subscribed_to maybe empty after on_state_change
        if self.subscribed_to:
            self._pubsub.subscribe(*self.subscribed_to)
        else:
            self._pubsub.connection = self._pubsub.connection_pool.get_connection(
                'pubsub', self._pubsub.shard_hint
            )
            # even if there is nothing to subscribe, we should not lose the callback after connecting.
            # The on_connect callback will re-subscribe to any channels we previously subscribed to.
            self._pubsub.connection.register_connect_callback(self._pubsub.on_connect)

    @contextmanager
    def reconnect_on_error(self):
        try:
            yield
        except self._connection_errors:
            try:
                self._ensure(self._reconnect_pubsub, ())
            except self._connection_errors:
                logger.critical(E_RETRY_LIMIT_EXCEEDED)
                raise

    def _maybe_cancel_ready_task(self, meta):
        if meta['status'] in states.READY_STATES:
            self.cancel_for(meta['task_id'])

    def on_state_change(self, meta, message):
        super().on_state_change(meta, message)
        self._maybe_cancel_ready_task(meta)

    def start(self, initial_task_id, **kwargs):
        self._pubsub = self.backend.client.pubsub(
            ignore_subscribe_messages=True,
        )
        self._consume_from(initial_task_id)

    def on_wait_for_pending(self, result, **kwargs):
        for meta in result._iter_meta(**kwargs):
            if meta is not None:
                self.on_state_change(meta, None)

    def stop(self):
        if self._pubsub is not None:
            self._pubsub.close()

    def drain_events(self, timeout=None):
        if self._pubsub:
            with self.reconnect_on_error():
                message = self._pubsub.get_message(timeout=timeout)
                if message and message['type'] == 'message':
                    self.on_state_change(self._decode_result(message['data']), message)
        elif timeout:
            time.sleep(timeout)

    def consume_from(self, task_id):
        if self._pubsub is None:
            return self.start(task_id)
        self._consume_from(task_id)

    def _consume_from(self, task_id):
        key = self._get_key_for_task(task_id)
        if key not in self.subscribed_to:
            self.subscribed_to.add(key)
            with self.reconnect_on_error():
                self._pubsub.subscribe(key)

    def cancel_for(self, task_id):
        key = self._get_key_for_task(task_id)
        self.subscribed_to.discard(key)
        if self._pubsub:
            with self.reconnect_on_error():
                self._pubsub.unsubscribe(key)


# Code mostly implemented from celery.backends.redis
class RedisClusterBackend(KeyValueStoreBackend):
    """Redis task result store."""

    ResultConsumer = ResultConsumer

    #: redis client module.
    redis = RedisCluster
    connection_class_ssl = connection.SSLConnection if redis else None

    startup_nodes = None
    max_connections = None
    init_slot_cache = True

    supports_autoexpire = True
    supports_native_join = True
    implements_incr = True

    def __init__(self, host=None, port=None, password=None,
                 max_connections=None, url=None, **kwargs):
        super().__init__(expires_type=int, **kwargs)

        if self.redis is None:
            raise ImproperlyConfigured(E_REDIS_MISSING)

        self.initialize_parameters(host, port, password, max_connections, url)

        # If we've received SSL parameters via query string or the
        # redis_backend_use_ssl dict, check ssl_cert_reqs is valid. If set
        # via query string ssl_cert_reqs will be a string so convert it here
        if ('connection_class' in self.connparams
                and issubclass(self.connparams['connection_class'], connection.SSLConnection)):
            ssl_cert_reqs_missing = 'MISSING'
            ssl_string_to_constant = {'CERT_REQUIRED': CERT_REQUIRED,
                                      'CERT_OPTIONAL': CERT_OPTIONAL,
                                      'CERT_NONE': CERT_NONE,
                                      'required': CERT_REQUIRED,
                                      'optional': CERT_OPTIONAL,
                                      'none': CERT_NONE}
            ssl_cert_reqs = self.connparams.get('ssl_cert_reqs', ssl_cert_reqs_missing)
            ssl_cert_reqs = ssl_string_to_constant.get(ssl_cert_reqs, ssl_cert_reqs)
            if ssl_cert_reqs not in ssl_string_to_constant.values():
                raise ValueError(E_REDIS_SSL_CERT_REQS_MISSING_INVALID)

            if ssl_cert_reqs == CERT_OPTIONAL:
                logger.warning(W_REDIS_SSL_CERT_OPTIONAL)
            elif ssl_cert_reqs == CERT_NONE:
                logger.warning(W_REDIS_SSL_CERT_NONE)
            self.connparams['ssl_cert_reqs'] = ssl_cert_reqs

        if self.connparams is not None and not isinstance(self.connparams, dict):
            raise ImproperlyConfigured(
                'RedisCluster backend settings should be grouped in a dict')

        try:
            new_join = strtobool(self.connparams.pop('new_join'))
            if new_join:
                self.apply_chord = self._new_chord_apply
                self.on_chord_part_return = self._new_chord_return

        except KeyError:
            pass

        self.expires = self.prepare_expires(None, type=int)
        self.connection_errors, self.channel_errors = (
            get_redis_error_classes() if get_redis_error_classes
            else ((), ()))
        self.result_consumer = self.ResultConsumer(
            self, self.app, self.accept,
            self._pending_results, self._pending_messages,
        )

    def initialize_parameters(self, host=None, port=None, password=None,
                              max_connections=None, url=None, **kwargs):
        _get = self.app.conf.get

        if host and '://' in host:
            url, host = host, None

        self.max_connections = (
            max_connections
            or _get('redis_max_connections')
            or self.max_connections)

        retry_on_timeout = _get('redis_retry_on_timeout')
        health_check_interval = _get('redis_backend_health_check_interval')
        skip_full_coverage_check = _get('skip_full_coverage_check')

        self.connparams = {
            'host': _get('redis_host') or 'localhost',
            'port': _get('redis_port') or 6379,
            'password': _get('redis_password'),
            'max_connections': self.max_connections,
            'retry_on_timeout': retry_on_timeout or False,
            'skip_full_coverage_check': skip_full_coverage_check or True
        }

        if health_check_interval:
            self.connparams["health_check_interval"] = health_check_interval

        # "redis_backend_use_ssl" must be a dict with the keys:
        # 'ssl_cert_reqs', 'ssl_ca_certs', 'ssl_certfile', 'ssl_keyfile'
        # (the same as "broker_use_ssl")
        ssl = _get('redis_backend_use_ssl')
        if ssl:
            self.connparams.update(ssl)
            self.connparams['connection_class'] = self.connection_class_ssl

        self.recommended_cluster_alias = _get('recommended_cluster_alias') or ('cluster-redis', 'cluster-rediss')
        self.other_cluster_alias = _get('other_cluster_alias') or ('redis', 'rediss')

        if not url:
            url = _get('redis_url')

        if url:
            self.connparams = self._params_from_url(url, self.connparams)

        self.url = url

    def get(self, key):
        return self.client.get(key)

    def mget(self, keys):
        return self.client.mget(keys)

    def ensure(self, fun, args, **policy):
        retry_policy = dict(self.retry_policy, **policy)
        max_retries = retry_policy.get('max_retries')
        return retry_over_time(
            fun, self.connection_errors, args, {},
            partial(self.on_connection_error, max_retries),
            **retry_policy
        )

    def on_task_call(self, producer, task_id):
        if not task_join_will_block():
            self.result_consumer.consume_from(task_id)

    def on_connection_error(self, max_retries, exc, intervals, retries):
        tts = next(intervals)
        logger.error(
            E_LOST.strip(),
            retries, max_retries or 'Inf', humanize_seconds(tts, 'in '))
        return tts

    def set(self, key, value, **retry_policy):
        return self.ensure(self._set, (key, value), **retry_policy)

    def _set(self, key, value):
        if hasattr(self, 'expires'):
            self.client.setex(key, self.expires, value)
        else:
            self.client.set(key, value)

    def forget(self, task_id):
        super().forget(task_id)
        self.result_consumer.cancel_for(task_id)

    def delete(self, key):
        self.client.delete(key)

    def incr(self, key):
        return self.client.incr(key)

    def expire(self, key, value):
        return self.client.expire(key, value)

    def add_to_chord(self, group_id, result):
        self.client.incr(self.get_key_for_group(group_id, '.t'), 1)

    def _unpack_chord_result(self, tup, decode,
                             EXCEPTION_STATES=states.EXCEPTION_STATES,
                             PROPAGATE_STATES=states.PROPAGATE_STATES):
        _, tid, state, retval = decode(tup)
        if state in EXCEPTION_STATES:
            retval = self.exception_to_python(retval)
        if state in PROPAGATE_STATES:
            raise ChordError('Dependency {0} raised {1!r}'.format(tid, retval))
        return retval

    def _new_chord_apply(self, header, partial_args, group_id, body,
                         result=None, options=None, **kwargs):
        # avoids saving the group in the redis db.
        options = options or {}
        options['task_id'] = group_id
        return header(*partial_args, **options or {})

    def _new_chord_return(self, task, state, result, propagate=None,
                          PROPAGATE_STATES=states.PROPAGATE_STATES):
        app = self.app
        if propagate is None:
            propagate = self.app.conf.CELERY_CHORD_PROPAGATES
        request = task.request
        tid, gid = request.id, request.group
        if not gid or not tid:
            return

        client = self.client
        jkey = self.get_key_for_group(gid, '.j')
        tkey = self.get_key_for_group(gid, '.t')
        result = self.encode_result(result, state)
        _, readycount, totaldiff, _, _ = client.pipeline()              \
            .rpush(jkey, self.encode([1, tid, state, result]))          \
            .llen(jkey)                                                 \
            .get(tkey)                                                  \
            .expire(jkey, 86400)                                        \
            .expire(tkey, 86400)                                        \
            .execute()

        totaldiff = int(totaldiff or 0)

        try:
            callback = maybe_signature(request.chord, app=app)
            total = callback['chord_size'] + totaldiff
            if readycount == total:
                decode, unpack = self.decode, self._unpack_chord_result
                resl, _, _ = client.pipeline()  \
                    .lrange(jkey, 0, total)     \
                    .delete(jkey)               \
                    .delete(tkey)               \
                    .execute()
                try:
                    callback.delay([unpack(tup, decode) for tup in resl])
                except Exception as exc:
                    error('Chord callback for %r raised: %r',
                          request.group, exc, exc_info=1)
                    app._tasks[callback.task].backend.fail_from_current_stack(
                        callback.id,
                        exc=ChordError('Callback error: {0!r}'.format(exc)),
                    )
        except ChordError as exc:
            error('Chord %r raised: %r', request.group, exc, exc_info=1)
            app._tasks[callback.task].backend.fail_from_current_stack(
                callback.id, exc=exc,
            )
        except Exception as exc:
            error('Chord %r raised: %r', request.group, exc, exc_info=1)
            app._tasks[callback.task].backend.fail_from_current_stack(
                callback.id, exc=ChordError('Join error: {0!r}'.format(exc)),
            )

    def _params_from_url(self, url, defaults):
        scheme, host, port, username, password, path, query = _parse_url(url)
        connparams = dict(
            defaults, **dictfilter({
                'host': host, 'port': port, 'username': username,
                'password': password})
        )

        ssl_param_keys = ['ssl_ca_certs', 'ssl_certfile', 'ssl_keyfile',
                          'ssl_cert_reqs']

        if scheme == self.recommended_cluster_alias[0] or scheme == self.other_cluster_alias[0]:
            # If connparams or query string contain ssl params, raise error
            if (any(key in connparams for key in ssl_param_keys)
                    or any(key in query for key in ssl_param_keys)):
                raise ValueError(E_REDIS_SSL_PARAMS_AND_SCHEME_MISMATCH)

        if scheme == self.recommended_cluster_alias[1] or scheme == self.other_cluster_alias[1]:
            connparams['connection_class'] = connection.SSLConnection
            connparams['ssl'] = True
            # The following parameters, if present in the URL, are encoded. We
            # must add the decoded values to connparams.
            for ssl_setting in ssl_param_keys:
                ssl_val = query.pop(ssl_setting, None)
                if ssl_val:
                    connparams[ssl_setting] = unquote(ssl_val)

        return connparams

    @cached_property
    def client(self):
        self.connparams.pop('connection_class', None)
        return RedisCluster(**self.connparams)
