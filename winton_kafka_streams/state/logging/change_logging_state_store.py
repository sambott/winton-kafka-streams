from typing import TypeVar, Iterator

from winton_kafka_streams.processor.serialization import Serde
from ..key_value_state_store import KeyValueStateStore
from ..state_store import StateStore
from .store_change_logger import StoreChangeLogger, StoreChangeLoggerImpl

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class ChangeLoggingStateStore(StateStore[KT, VT]):
    def __init__(self,  name: str, key_serde: Serde[KT], value_serde: Serde[VT], logging_enabled: bool,
                 inner_state_store: StateStore[KT, VT]) -> None:
        super().__init__(name, key_serde, value_serde, logging_enabled)
        self.inner_state_store = inner_state_store
        self.change_logger: StoreChangeLogger = None

    def _get_change_logger(self, context) -> StoreChangeLogger:
        return StoreChangeLoggerImpl(self.inner_state_store.name, context)

    def initialize(self, context, root):
        self.inner_state_store.initialize(context, root)
        self.change_logger = self._get_change_logger(context)
        for k, v in self.change_logger:
            deserialized_key = self.deserialize_key(k)
            inner_kv_store = self.inner_state_store.get_key_value_store()
            if v == b'':
                del inner_kv_store[deserialized_key]
            else:
                inner_kv_store[deserialized_key] = self.deserialize_value(v)

    def get_key_value_store(self) -> KeyValueStateStore[KT, VT]:
        parent = self

        class ChangeLoggingKeyValueStore(KeyValueStateStore[KT, VT]):
            # TODO : add write buffer
            # TODO : use topic compaction to optimise state-rebuilding

            def __init__(self, change_logger: StoreChangeLogger) -> None:
                super(ChangeLoggingKeyValueStore, self).__init__()
                self.change_logger: StoreChangeLogger = change_logger
                self.inner_kv_store: KeyValueStateStore[KT, VT] = parent.inner_state_store.get_key_value_store()

            def __len__(self) -> int:
                return len(self.inner_kv_store)

            def __iter__(self) -> Iterator[KT]:
                return self.inner_kv_store.__iter__()

            def __setitem__(self, key: KT, value: VT):
                key_bytes = parent.serialize_key(key)
                value_bytes = parent.serialize_value(value)
                self.inner_kv_store.__setitem__(key, value)
                self.change_logger.log_change(key_bytes, value_bytes)

            def __getitem__(self, key: KT) -> VT:
                return self.inner_kv_store.__getitem__(key)

            def __delitem__(self, key: KT):
                key_bytes = parent.serialize_key(key)
                self.inner_kv_store.__delitem__(key)
                self.change_logger.log_change(key_bytes, b'')

        return ChangeLoggingKeyValueStore(self.change_logger)
