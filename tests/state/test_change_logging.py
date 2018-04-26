from collections import deque
from typing import Iterator, Tuple

import pytest

from winton_kafka_streams.processor.serialization.serdes import IntegerSerde, StringSerde
from winton_kafka_streams.state.in_memory.in_memory_state_store import InMemoryStateStore
from winton_kafka_streams.state.logging.change_logging_state_store import ChangeLoggingStateStore
from winton_kafka_streams.state.logging.store_change_logger import StoreChangeLogger


class MockChangeLogger(StoreChangeLogger):
    def __init__(self):
        super(MockChangeLogger, self).__init__()
        self.change_log = deque()

    def log_change(self, key: bytes, value: bytes) -> None:
        self.change_log.append((key, value))

    def __iter__(self) -> Iterator[Tuple[bytes, bytes]]:
        return self.change_log.__iter__()


def _get_store():
    inner_store = InMemoryStateStore('teststore', StringSerde(), IntegerSerde(), False)
    store = ChangeLoggingStateStore('teststore', StringSerde(), IntegerSerde(), False, inner_store)
    store._get_change_logger = lambda context: MockChangeLogger()
    store.initialize(None, None)
    return store


def test_change_store_is_dict():
    store = _get_store()
    kv_store = store.get_key_value_store()

    kv_store['a'] = 1
    assert kv_store['a'] == 1

    kv_store['a'] = 2
    assert kv_store['a'] == 2

    del kv_store['a']
    assert kv_store.get('a') is None
    with pytest.raises(KeyError):
        _ = kv_store['a']


def test_change_log_is_written_to():
    store = _get_store()
    kv_store = store.get_key_value_store()

    kv_store['a'] = 12
    assert len(store.change_logger.change_log) == 1
    assert store.change_logger.change_log[0] == (b'a', b'\x0c\0\0\0')

    del kv_store['a']
    assert len(store.change_logger.change_log) == 2
    assert store.change_logger.change_log[1] == (b'a', b'')


def test_can_replay_log():
    store = _get_store()
    kv_store = store.get_key_value_store()

    kv_store['a'] = 12
    kv_store['b'] = 123
    del kv_store['a']

    keys = []
    values = []

    for k, v in store.change_logger:
        keys.append(k)
        values.append(v)

    assert keys == [b'a', b'b', b'a']
    assert values == [b'\x0c\0\0\0', b'\x7b\0\0\0', b'']


def test_rebuild_state_from_log():
    store = _get_store()
    kv_store = store.get_key_value_store()

    kv_store['a'] = 12
    kv_store['b'] = 123
    del kv_store['a']
    kv_store['c'] = 1234

    log = store.change_logger

    # reattach previous changelog and run initialize()
    store = _get_store()
    kv_store = store.get_key_value_store()
    store._get_change_logger = lambda context: log
    store.initialize(None, None)

    with pytest.raises(KeyError):
        _ = kv_store['a']
    assert kv_store['b'] == 123
    assert kv_store['c'] == 1234
