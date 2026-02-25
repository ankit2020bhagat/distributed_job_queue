

import json
import asyncio
from typing import Dict, Any, Optional, Callable
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError

from app.core.config import settings


class KafkaProducerService:


    def __init__(self):
        """Initialize Kafka producer"""
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self):

        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                compression_type='gzip',
                acks='all',  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=5,
            )
            await self._producer.start()
            print("✓ Kafka producer started")

    async def stop(self):

        if self._producer:
            await self._producer.stop()
            self._producer = None
            print("✓ Kafka producer stopped")

    async def publish_job(
            self,
            job_id: str,
            job_data: Dict[str, Any],
            priority: str = "MEDIUM"
    ) -> bool:

        try:
            topic = settings.get_kafka_topic(priority)

            message = {
                "job_id": job_id,
                "job_data": job_data,
                "timestamp": asyncio.get_event_loop().time()
            }

            # Send message to Kafka
            await self._producer.send_and_wait(topic, message)

            print(f"✓ Published job {job_id} to topic {topic}")
            return True

        except KafkaError as e:
            print(f"✗ Failed to publish job {job_id}: {e}")
            return False
        except Exception as e:
            print(f"✗ Unexpected error publishing job {job_id}: {e}")
            return False

    async def health_check(self) -> bool:

        try:
            if self._producer and not self._producer._closed:
                return True
            return False
        except Exception:
            return False


class KafkaConsumerService:


    def __init__(self, priority: str, group_id: Optional[str] = None):

        self.priority = priority.upper()
        self.topic = settings.get_kafka_topic(self.priority)
        self.group_id = group_id or settings.kafka.consumer_group
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False

    async def start(self):

        if self._consumer is None:
            self._consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=settings.kafka.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=settings.kafka.auto_offset_reset,
                enable_auto_commit=settings.kafka.enable_auto_commit,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                max_poll_records=settings.worker.batch_size,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            await self._consumer.start()
            print(f"✓ Kafka consumer started for topic {self.topic}")

    async def stop(self):
        """Stop Kafka consumer and cleanup"""
        self._running = False
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
            print(f"✓ Kafka consumer stopped for topic {self.topic}")

    async def consume(
            self,
            handler: Callable[[Dict[str, Any]], asyncio.coroutine]
    ):

        self._running = True

        try:
            while self._running:
                try:
                    # Poll for messages
                    result = await self._consumer.getmany(
                        timeout_ms=int(settings.worker.poll_interval * 1000),
                        max_records=settings.worker.batch_size
                    )

                    # Process messages
                    for topic_partition, messages in result.items():
                        for message in messages:
                            try:
                                # Call handler
                                success = await handler(message.value)

                                if success:
                                    # Commit offset on success
                                    await self._consumer.commit({
                                        topic_partition: message.offset + 1
                                    })
                                else:
                                    print(f"Handler failed for message: {message.value}")

                            except Exception as e:
                                print(f"Error processing message: {e}")
                                # Don't commit offset on error
                                # Message will be reprocessed

                except asyncio.CancelledError:
                    print(f"Consumer cancelled for topic {self.topic}")
                    break
                except KafkaError as e:
                    print(f"Kafka error in consumer: {e}")
                    await asyncio.sleep(5)  # Back off on error
                except Exception as e:
                    print(f"Unexpected error in consumer: {e}")
                    await asyncio.sleep(5)

        finally:
            self._running = False

    async def health_check(self) -> bool:

        try:
            if self._consumer and not self._consumer._closed:
                return True
            return False
        except Exception:
            return False


# Global producer instance
kafka_producer = KafkaProducerService()


async def init_kafka_producer():
    """Initialize Kafka producer"""
    await kafka_producer.start()


async def close_kafka_producer():
    """Close Kafka producer"""
    await kafka_producer.stop()