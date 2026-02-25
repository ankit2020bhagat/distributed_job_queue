

import uuid
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import Job, JobStatus, JobPriority, JobHistory
from app.schemas.job import JobCreate, JobStats
from app.core.config import settings
from app.core.redis import redis_service
from app.core.kafka import kafka_producer


class JobService:


    @staticmethod
    async def create_job(
            db: AsyncSession,
            job_data: JobCreate,
            scheduled_job_id: Optional[str] = None
    ) -> Job:

        job_id = str(uuid.uuid4())

        # Create job instance
        job = Job(
            id=job_id,
            name=job_data.name,
            priority=JobPriority[job_data.priority.value],
            status=JobStatus.PENDING,
            payload=job_data.payload,
            max_retries=job_data.max_retries,
            retry_count=0,
            scheduled_job_id=scheduled_job_id,
        )

        # Save to database
        db.add(job)
        await db.flush()

        # Create history entry
        history = JobHistory(
            job_id=job_id,
            from_status=None,
            to_status=JobStatus.PENDING,
            message="Job created"
        )
        db.add(history)
        await db.commit()

        # Cache in Redis
        await redis_service.cache_job(job_id, job.to_dict())

        # Increment queue size
        await redis_service.increment_queue_size(job.priority.value)

        # Publish to Kafka
        await kafka_producer.publish_job(
            job_id,
            job.to_dict(),
            job.priority.value
        )

        print(f"✓ Created job {job_id} ({job.name}) with priority {job.priority.value}")

        return job

    @staticmethod
    async def get_job(
            db: AsyncSession,
            job_id: str,
            use_cache: bool = True
    ) -> Optional[Job]:

        if use_cache:
            cached = await redis_service.get_cached_job(job_id)
            if cached:
                # Reconstruct job object from cache
                # (In production, you might use full object caching)
                pass

        # Query database
        result = await db.execute(
            select(Job).where(Job.id == job_id)
        )
        job = result.scalar_one_or_none()

        # Update cache if found
        if job and use_cache:
            await redis_service.cache_job(job_id, job.to_dict())

        return job

    @staticmethod
    async def get_jobs(
            db: AsyncSession,
            status: Optional[JobStatus] = None,
            priority: Optional[JobPriority] = None,
            limit: int = 100,
            offset: int = 0
    ) -> List[Job]:

        query = select(Job)

        # Apply filters
        conditions = []
        if status:
            conditions.append(Job.status == status)
        if priority:
            conditions.append(Job.priority == priority)

        if conditions:
            query = query.where(and_(*conditions))

        # Order by priority (descending) and creation time
        query = query.order_by(
            Job.priority.desc(),
            Job.created_at.asc()
        ).limit(limit).offset(offset)

        result = await db.execute(query)
        return list(result.scalars().all())

    @staticmethod
    async def update_job_status(
            db: AsyncSession,
            job_id: str,
            new_status: JobStatus,
            error_message: Optional[str] = None,
            result: Optional[Dict[str, Any]] = None
    ) -> Optional[Job]:

        job = await JobService.get_job(db, job_id, use_cache=False)
        if not job:
            return None

        old_status = job.status
        job.status = new_status

        # Update timestamps based on status
        if new_status == JobStatus.PROCESSING:
            job.started_at = datetime.utcnow()
            await redis_service.decrement_queue_size(job.priority.value)

        elif new_status in [JobStatus.COMPLETED, JobStatus.FAILED]:
            job.completed_at = datetime.utcnow()
            if job.started_at:
                job.execution_time = (
                        job.completed_at - job.started_at
                ).total_seconds()

        # Set error or result
        if error_message:
            job.error_message = error_message
        if result:
            job.result = result

        # Create history entry
        history = JobHistory(
            job_id=job_id,
            from_status=old_status,
            to_status=new_status,
            message=error_message or f"Status changed to {new_status.value}"
        )
        db.add(history)

        await db.commit()
        await db.refresh(job)

        # Update cache
        await redis_service.cache_job(job_id, job.to_dict())

        print(f"✓ Updated job {job_id}: {old_status.value} -> {new_status.value}")

        return job

    @staticmethod
    async def schedule_retry(
            db: AsyncSession,
            job_id: str
    ) -> Optional[Job]:

        job = await JobService.get_job(db, job_id, use_cache=False)
        if not job:
            return None

        # Check if retries exhausted
        if job.retry_count >= job.max_retries:
            # Move to DLQ
            await JobService.move_to_dlq(db, job_id)
            return job

        # Increment retry count
        job.retry_count += 1

        # Calculate backoff delay
        delay = settings.retry.calculate_delay(job.retry_count)

        # Schedule next retry
        job.next_retry_at = datetime.utcnow() + timedelta(seconds=delay)
        job.status = JobStatus.PENDING

        # Create history
        history = JobHistory(
            job_id=job_id,
            from_status=JobStatus.FAILED,
            to_status=JobStatus.PENDING,
            message=f"Retry {job.retry_count}/{job.max_retries} scheduled in {delay}s"
        )
        db.add(history)

        await db.commit()
        await db.refresh(job)

        # Re-publish to Kafka
        await kafka_producer.publish_job(
            job_id,
            job.to_dict(),
            job.priority.value
        )

        print(f"✓ Scheduled retry for job {job_id} (attempt {job.retry_count}/{job.max_retries})")

        return job

    @staticmethod
    async def move_to_dlq(db: AsyncSession, job_id: str) -> Optional[Job]:

        job = await JobService.get_job(db, job_id, use_cache=False)
        if not job:
            return None

        job.status = JobStatus.FAILED

        # Create history
        history = JobHistory(
            job_id=job_id,
            from_status=job.status,
            to_status=JobStatus.FAILED,
            message="Moved to DLQ - max retries exceeded"
        )
        db.add(history)

        await db.commit()
        await db.refresh(job)

        print(f"✗ Moved job {job_id} to DLQ")

        return job

    @staticmethod
    async def get_statistics(db: AsyncSession) -> JobStats:

        # Total jobs
        total_result = await db.execute(
            select(func.count(Job.id))
        )
        total_jobs = total_result.scalar() or 0

        # Count by status
        status_counts = await db.execute(
            select(
                Job.status,
                func.count(Job.id)
            ).group_by(Job.status)
        )

        status_map = {status: count for status, count in status_counts.all()}

        pending = status_map.get(JobStatus.PENDING, 0)
        processing = status_map.get(JobStatus.PROCESSING, 0)
        completed = status_map.get(JobStatus.COMPLETED, 0)
        failed = status_map.get(JobStatus.FAILED, 0)

        # Success rate
        total_finished = completed + failed
        success_rate = (completed / total_finished * 100) if total_finished > 0 else 0.0

        # Average execution time
        avg_time_result = await db.execute(
            select(func.avg(Job.execution_time)).where(
                Job.execution_time.isnot(None)
            )
        )
        avg_execution_time = avg_time_result.scalar()

        stats = JobStats(
            total_jobs=total_jobs,
            pending_jobs=pending,
            processing_jobs=processing,
            completed_jobs=completed,
            failed_jobs=failed,
            success_rate=round(success_rate, 2),
            avg_execution_time=round(avg_execution_time, 2) if avg_execution_time else None
        )

        # Cache statistics
        await redis_service.update_stats(stats.dict())

        return stats

    @staticmethod
    async def reset_zombie_jobs(db: AsyncSession) -> int:

        threshold = datetime.utcnow() - timedelta(
            seconds=settings.job.zombie_threshold
        )

        result = await db.execute(
            select(Job).where(
                and_(
                    Job.status == JobStatus.PROCESSING,
                    Job.started_at < threshold
                )
            )
        )
        zombie_jobs = list(result.scalars().all())

        for job in zombie_jobs:
            await JobService.update_job_status(
                db,
                job.id,
                JobStatus.PENDING,
                error_message="Reset from zombie state"
            )

            # Re-publish to Kafka
            await kafka_producer.publish_job(
                job.id,
                job.to_dict(),
                job.priority.value
            )

        if zombie_jobs:
            print(f"✓ Reset {len(zombie_jobs)} zombie jobs")

        return len(zombie_jobs)