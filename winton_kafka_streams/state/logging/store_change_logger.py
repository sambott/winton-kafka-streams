from abc import abstractmethod
from typing import Iterator, Iterable, Tuple

from confluent_kafka.cimpl import TopicPartition, OFFSET_BEGINNING, KafkaError

from winton_kafka_streams.processor.serialization.serdes import BytesSerde
from winton_kafka_streams.kafka_client_supplier import KafkaClientSupplier
from winton_kafka_streams.processor._record_collector import RecordCollector


class StoreChangeLogger(Iterable[Tuple[bytes, bytes]]):
    @abstractmethod
    def log_change(self, key: bytes, value: bytes) -> None:
        pass

    @abstractmethod
    def __iter__(self) -> Iterator[Tuple[bytes, bytes]]:
        pass


class StoreChangeLoggerImpl(StoreChangeLogger):
    def __init__(self, store_name, context) -> None:
        self.topic = f'{context.application_id}-{store_name}-changelog'
        self.context = context
        self.partition = context.task_id.partition
        self.client_supplier = KafkaClientSupplier(self.context.config)
        self.record_collector = RecordCollector(self.client_supplier.producer(), BytesSerde(), BytesSerde())

    def log_change(self, key: bytes, value: bytes) -> None:
        if self.record_collector:
            self.record_collector.send(self.topic, key, value, self.context.timestamp, partition=self.partition)

    def __iter__(self) -> Iterator[Tuple[bytes, bytes]]:
        consumer = self.client_supplier.consumer()
        partition = TopicPartition(self.topic, self.partition, OFFSET_BEGINNING)
        consumer.assign([partition])

        class TopicIterator(Iterator[Tuple[bytes, bytes]]):
            def __next__(self) -> Tuple[bytes, bytes]:
                msg = consumer.poll(1.0)
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        raise StopIteration()
                if msg is None:
                    raise StopIteration()
                return msg.key(), msg.value()

        return TopicIterator()
