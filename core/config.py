

from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field, validator


class DatabaseSettings(BaseSettings):
    """Database configuration settings"""

    url: str = Field(
        default="postgresql+asyncpg://jobqueue:jobqueue123@localhost:5432/jobqueue_db",
        description="PostgreSQL connection URL"
    )
    pool_size: int = Field(default=20, description="Connection pool size")
    max_overflow: int = Field(default=10, description="Max connections over pool size")

    class Config:
        env_prefix = "DATABASE_"


class RedisSettings(BaseSettings):
    """Redis configuration settings"""

    url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL"
    )
    max_connections: int = Field(default=50, description="Maximum Redis connections")
    decode_responses: bool = Field(default=True, description="Decode Redis responses to strings")

    class Config:
        env_prefix = "REDIS_"


class KafkaSettings(BaseSettings):
    """Kafka configuration settings"""

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka broker addresses"
    )
    topic_high: str = Field(default="jobs-high-priority", description="High priority topic")
    topic_medium: str = Field(default="jobs-medium-priority", description="Medium priority topic")
    topic_low: str = Field(default="jobs-low-priority", description="Low priority topic")
    consumer_group: str = Field(default="job-queue-workers", description="Consumer group ID")
    auto_offset_reset: str = Field(default="earliest", description="Offset reset strategy")
    enable_auto_commit: bool = Field(default=False, description="Auto-commit offsets")

    class Config:
        env_prefix = "KAFKA_"

    @validator('bootstrap_servers')
    def validate_servers(cls, v):
        """Ensure bootstrap servers are properly formatted"""
        if not v or not v.strip():
            raise ValueError("Kafka bootstrap servers cannot be empty")
        return v


class WorkerSettings(BaseSettings):
    """Worker pool configuration settings"""

    max_workers_high: int = Field(default=5, description="Max workers for high priority")
    max_workers_medium: int = Field(default=3, description="Max workers for medium priority")
    max_workers_low: int = Field(default=2, description="Max workers for low priority")
    poll_interval: float = Field(default=1.0, description="Kafka polling interval (seconds)")
    batch_size: int = Field(default=10, description="Max messages per poll")

    class Config:
        env_prefix = "WORKER_"


class RetrySettings(BaseSettings):
    """Job retry configuration settings"""

    base_delay: int = Field(default=5, description="Base delay for exponential backoff (seconds)")
    max_delay: int = Field(default=300, description="Maximum retry delay (seconds)")
    multiplier: int = Field(default=2, description="Exponential backoff multiplier")
    default_max_retries: int = Field(default=3, description="Default max retry attempts")

    class Config:
        env_prefix = "RETRY_"

    def calculate_delay(self, retry_count: int) -> int:

        import random
        delay = min(
            self.base_delay * (self.multiplier ** retry_count),
            self.max_delay
        )
        # Add jitter (±20% randomness) to prevent thundering herd
        jitter = random.uniform(0.8, 1.2)
        return int(delay * jitter)


class SchedulerSettings(BaseSettings):
    """Scheduler configuration settings"""

    interval: int = Field(default=60, description="Check interval (seconds)")
    timezone: str = Field(default="UTC", description="Default timezone")

    class Config:
        env_prefix = "SCHEDULER_"


class JobSettings(BaseSettings):
    """Job execution configuration settings"""

    timeout: int = Field(default=3600, description="Default job timeout (seconds)")
    max_execution_time: int = Field(default=7200, description="Maximum job execution time (seconds)")
    zombie_threshold: int = Field(default=3600, description="Time before job considered zombie (seconds)")

    class Config:
        env_prefix = "JOB_"


class DLQSettings(BaseSettings):
    """Dead Letter Queue configuration settings"""

    enabled: bool = Field(default=True, description="Enable DLQ")
    retention_days: int = Field(default=7, description="DLQ retention period (days)")

    class Config:
        env_prefix = "DLQ_"


class Settings(BaseSettings):


    # Application
    app_name: str = Field(default="Distributed Job Queue", description="Application name")
    app_version: str = Field(default="1.0.0", description="Application version")
    debug: bool = Field(default=False, description="Debug mode")
    log_level: str = Field(default="INFO", description="Logging level")

    # Server
    host: str = Field(default="0.0.0.0", description="Server host")
    port: int = Field(default=8000, description="Server port")

    # CORS
    cors_origins: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:8000"],
        description="Allowed CORS origins"
    )
    cors_allow_credentials: bool = Field(default=True, description="Allow credentials")
    cors_allow_methods: List[str] = Field(default=["*"], description="Allowed methods")
    cors_allow_headers: List[str] = Field(default=["*"], description="Allowed headers")

    # Nested configurations
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    retry: RetrySettings = Field(default_factory=RetrySettings)
    scheduler: SchedulerSettings = Field(default_factory=SchedulerSettings)
    job: JobSettings = Field(default_factory=JobSettings)
    dlq: DLQSettings = Field(default_factory=DLQSettings)

    # Monitoring
    metrics_enabled: bool = Field(default=True, description="Enable metrics")
    metrics_port: int = Field(default=9090, description="Metrics port")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    def get_kafka_topic(self, priority: str) -> str:
        """
        Get Kafka topic name for given priority level.

        Args:
            priority: Priority level (HIGH, MEDIUM, LOW)

        Returns:
            Kafka topic name
        """
        topic_map = {
            "HIGH": self.kafka.topic_high,
            "MEDIUM": self.kafka.topic_medium,
            "LOW": self.kafka.topic_low,
        }
        return topic_map.get(priority.upper(), self.kafka.topic_medium)

    def get_max_workers(self, priority: str) -> int:

        worker_map = {
            "HIGH": self.worker.max_workers_high,
            "MEDIUM": self.worker.max_workers_medium,
            "LOW": self.worker.max_workers_low,
        }
        return worker_map.get(priority.upper(), self.worker.max_workers_medium)


# Global settings instance
settings = Settings()