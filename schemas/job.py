

from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


class JobPriorityEnum(str, Enum):
    """Job priority levels for API"""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class JobStatusEnum(str, Enum):
    """Job status values for API"""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SCHEDULED = "SCHEDULED"
    CANCELLED = "CANCELLED"



class JobCreate(BaseModel):

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Job name/type identifier"
    )
    priority: JobPriorityEnum = Field(
        default=JobPriorityEnum.MEDIUM,
        description="Job priority level"
    )
    payload: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Job execution data"
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum retry attempts"
    )

    @validator('name')
    def validate_name(cls, v):
        """Ensure job name is alphanumeric with underscores"""
        if not v.replace('_', '').replace('-', '').isalnum():
            raise ValueError("Job name must contain only alphanumeric characters, hyphens, and underscores")
        return v

    class Config:
        schema_extra = {
            "example": {
                "name": "send_email",
                "priority": "HIGH",
                "payload": {
                    "to": "user@example.com",
                    "subject": "Welcome",
                    "template": "welcome_email"
                },
                "max_retries": 3
            }
        }


class JobResponse(BaseModel):

    id: str
    name: str
    priority: str
    status: str
    payload: Optional[Dict[str, Any]]
    result: Optional[Dict[str, Any]]
    error_message: Optional[str]
    max_retries: int
    retry_count: int
    next_retry_at: Optional[datetime]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    execution_time: Optional[float]
    created_at: datetime
    updated_at: datetime
    scheduled_job_id: Optional[str]

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "name": "send_email",
                "priority": "HIGH",
                "status": "COMPLETED",
                "payload": {"to": "user@example.com"},
                "result": {"sent": True, "message_id": "abc123"},
                "error_message": None,
                "max_retries": 3,
                "retry_count": 0,
                "next_retry_at": None,
                "started_at": "2024-01-01T10:00:00Z",
                "completed_at": "2024-01-01T10:00:05Z",
                "execution_time": 5.2,
                "created_at": "2024-01-01T09:59:55Z",
                "updated_at": "2024-01-01T10:00:05Z",
                "scheduled_job_id": None
            }
        }


class JobListResponse(BaseModel):
    """
    Schema for paginated job list.
    """
    jobs: List[JobResponse]
    total: int
    page: int
    page_size: int
    total_pages: int




class ScheduledJobCreate(BaseModel):

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Job name/type"
    )
    cron_expression: str = Field(
        ...,
        description="Cron expression (e.g., '0 9 * * *' for daily at 9 AM)"
    )
    payload: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Default payload for created jobs"
    )
    priority: JobPriorityEnum = Field(
        default=JobPriorityEnum.MEDIUM,
        description="Priority for created jobs"
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Max retries for created jobs"
    )
    is_active: bool = Field(
        default=True,
        description="Whether schedule is active"
    )

    @validator('cron_expression')
    def validate_cron(cls, v):
        """Validate cron expression format"""
        from croniter import croniter
        if not croniter.is_valid(v):
            raise ValueError("Invalid cron expression")
        return v

    class Config:
        schema_extra = {
            "example": {
                "name": "daily_backup",
                "cron_expression": "0 2 * * *",
                "payload": {"backup_type": "incremental"},
                "priority": "MEDIUM",
                "max_retries": 3,
                "is_active": True
            }
        }


class ScheduledJobResponse(BaseModel):
    """Schema for scheduled job response"""
    id: str
    name: str
    cron_expression: str
    payload: Optional[Dict[str, Any]]
    priority: str
    max_retries: int
    is_active: bool
    last_run_at: Optional[datetime]
    next_run_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class ScheduledJobUpdate(BaseModel):
    """Schema for updating a scheduled job"""
    cron_expression: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None
    priority: Optional[JobPriorityEnum] = None
    max_retries: Optional[int] = Field(None, ge=0, le=10)
    is_active: Optional[bool] = None

    @validator('cron_expression')
    def validate_cron(cls, v):
        """Validate cron expression if provided"""
        if v is not None:
            from croniter import croniter
            if not croniter.is_valid(v):
                raise ValueError("Invalid cron expression")
        return v




class JobStats(BaseModel):

    total_jobs: int = Field(description="Total jobs in system")
    pending_jobs: int = Field(description="Jobs waiting to be processed")
    processing_jobs: int = Field(description="Jobs currently being processed")
    completed_jobs: int = Field(description="Successfully completed jobs")
    failed_jobs: int = Field(description="Failed jobs in DLQ")
    success_rate: float = Field(description="Success rate percentage")
    avg_execution_time: Optional[float] = Field(description="Average job execution time (seconds)")

    class Config:
        schema_extra = {
            "example": {
                "total_jobs": 1000,
                "pending_jobs": 50,
                "processing_jobs": 10,
                "completed_jobs": 920,
                "failed_jobs": 20,
                "success_rate": 97.87,
                "avg_execution_time": 12.5
            }
        }


class WorkerPoolStatus(BaseModel):

    priority: str
    active_workers: int
    max_workers: int
    queue_size: int

    class Config:
        schema_extra = {
            "example": {
                "priority": "HIGH",
                "active_workers": 4,
                "max_workers": 5,
                "queue_size": 15
            }
        }


class DashboardMetrics(BaseModel):

    stats: JobStats
    worker_pools: List[WorkerPoolStatus]
    recent_jobs: List[JobResponse]
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        schema_extra = {
            "example": {
                "stats": {
                    "total_jobs": 1000,
                    "pending_jobs": 50,
                    "processing_jobs": 10,
                    "completed_jobs": 920,
                    "failed_jobs": 20,
                    "success_rate": 97.87,
                    "avg_execution_time": 12.5
                },
                "worker_pools": [
                    {
                        "priority": "HIGH",
                        "active_workers": 4,
                        "max_workers": 5,
                        "queue_size": 15
                    }
                ],
                "recent_jobs": [],
                "timestamp": "2024-01-01T10:00:00Z"
            }
        }


class ErrorResponse(BaseModel):
    """Standard error response"""
    error: str
    detail: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        schema_extra = {
            "example": {
                "error": "Job not found",
                "detail": "Job with ID 123 does not exist",
                "timestamp": "2024-01-01T10:00:00Z"
            }
        }