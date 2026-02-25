

from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum as PyEnum

from sqlalchemy import (
    Column, String, Integer, DateTime, JSON, Text,
    Index, Enum, Boolean, Float
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class JobPriority(str, PyEnum):

    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class JobStatus(str, PyEnum):

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SCHEDULED = "SCHEDULED"
    CANCELLED = "CANCELLED"


class Job(Base):


    __tablename__ = "jobs"

    # Primary Key
    id = Column(String(36), primary_key=True)

    # Job Metadata
    name = Column(String(255), nullable=False, index=True)
    priority = Column(Enum(JobPriority), nullable=False, default=JobPriority.MEDIUM)
    status = Column(Enum(JobStatus), nullable=False, default=JobStatus.PENDING, index=True)

    # Job Data
    payload = Column(JSON, nullable=True)
    result = Column(JSON, nullable=True)
    error_message = Column(Text, nullable=True)

    # Retry Configuration
    max_retries = Column(Integer, nullable=False, default=3)
    retry_count = Column(Integer, nullable=False, default=0)
    next_retry_at = Column(DateTime(timezone=True), nullable=True)

    # Timing
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    execution_time = Column(Float, nullable=True)  # in seconds

    # Timestamps
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now()
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    )

    # Relationships
    scheduled_job_id = Column(String(36), nullable=True, index=True)

    # Indexes for performance
    __table_args__ = (
        Index('idx_job_queue', 'status', 'priority', 'created_at'),
        Index('idx_job_retry', 'status', 'next_retry_at'),
        Index('idx_job_created', 'created_at'),
    )

    def __repr__(self) -> str:
        return (
            f"<Job(id={self.id}, name={self.name}, "
            f"priority={self.priority}, status={self.status})>"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary representation"""
        return {
            "id": self.id,
            "name": self.name,
            "priority": self.priority.value,
            "status": self.status.value,
            "payload": self.payload,
            "result": self.result,
            "error_message": self.error_message,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "next_retry_at": self.next_retry_at.isoformat() if self.next_retry_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "execution_time": self.execution_time,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "scheduled_job_id": self.scheduled_job_id,
        }


class ScheduledJob(Base):


    __tablename__ = "scheduled_jobs"

    id = Column(String(36), primary_key=True)
    name = Column(String(255), nullable=False, index=True)
    cron_expression = Column(String(100), nullable=False)

    # Job Configuration
    payload = Column(JSON, nullable=True)
    priority = Column(Enum(JobPriority), nullable=False, default=JobPriority.MEDIUM)
    max_retries = Column(Integer, nullable=False, default=3)

    # Schedule State
    is_active = Column(Boolean, nullable=False, default=True)
    last_run_at = Column(DateTime(timezone=True), nullable=True)
    next_run_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Timestamps
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now()
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    )

    __table_args__ = (
        Index('idx_scheduled_active', 'is_active', 'next_run_at'),
    )

    def __repr__(self) -> str:
        return (
            f"<ScheduledJob(id={self.id}, name={self.name}, "
            f"cron={self.cron_expression}, active={self.is_active})>"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert scheduled job to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "cron_expression": self.cron_expression,
            "payload": self.payload,
            "priority": self.priority.value,
            "max_retries": self.max_retries,
            "is_active": self.is_active,
            "last_run_at": self.last_run_at.isoformat() if self.last_run_at else None,
            "next_run_at": self.next_run_at.isoformat() if self.next_run_at else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class JobHistory(Base):


    __tablename__ = "job_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(36), nullable=False, index=True)

    from_status = Column(Enum(JobStatus), nullable=True)
    to_status = Column(Enum(JobStatus), nullable=False)

    message = Column(Text, nullable=True)
    metadata = Column(JSON, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True
    )

    __table_args__ = (
        Index('idx_history_job', 'job_id', 'created_at'),
    )

    def __repr__(self) -> str:
        return (
            f"<JobHistory(job_id={self.job_id}, "
            f"{self.from_status} -> {self.to_status})>"
        )