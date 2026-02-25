
import uuid
from datetime import datetime, timezone
from typing import Optional, List
from croniter import croniter
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import ScheduledJob, JobPriority
from app.schemas.job import ScheduledJobCreate, ScheduledJobUpdate
from app.services.job_service import JobService
from app.schemas.job import JobCreate


class ScheduledJobService:


    @staticmethod
    async def create_scheduled_job(
            db: AsyncSession,
            job_data: ScheduledJobCreate
    ) -> ScheduledJob:

        # Generate unique ID
        schedule_id = str(uuid.uuid4())

        # Calculate next run time
        now = datetime.now(timezone.utc)
        cron = croniter(job_data.cron_expression, now)
        next_run = cron.get_next(datetime)

        # Create scheduled job
        scheduled_job = ScheduledJob(
            id=schedule_id,
            name=job_data.name,
            cron_expression=job_data.cron_expression,
            payload=job_data.payload,
            priority=JobPriority[job_data.priority.value],
            max_retries=job_data.max_retries,
            is_active=job_data.is_active,
            next_run_at=next_run,
        )

        db.add(scheduled_job)
        await db.commit()
        await db.refresh(scheduled_job)

        print(f"✓ Created scheduled job {schedule_id} ({scheduled_job.name})")
        print(f"  Cron: {job_data.cron_expression}")
        print(f"  Next run: {next_run}")

        return scheduled_job

    @staticmethod
    async def get_scheduled_job(
            db: AsyncSession,
            schedule_id: str
    ) -> Optional[ScheduledJob]:

        result = await db.execute(
            select(ScheduledJob).where(ScheduledJob.id == schedule_id)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def get_all_scheduled_jobs(
            db: AsyncSession,
            active_only: bool = False
    ) -> List[ScheduledJob]:

        query = select(ScheduledJob)

        if active_only:
            query = query.where(ScheduledJob.is_active == True)

        query = query.order_by(ScheduledJob.next_run_at.asc())

        result = await db.execute(query)
        return list(result.scalars().all())

    @staticmethod
    async def update_scheduled_job(
            db: AsyncSession,
            schedule_id: str,
            update_data: ScheduledJobUpdate
    ) -> Optional[ScheduledJob]:

        scheduled_job = await ScheduledJobService.get_scheduled_job(db, schedule_id)
        if not scheduled_job:
            return None

        # Update fields
        update_dict = update_data.dict(exclude_unset=True)

        for field, value in update_dict.items():
            if value is not None:
                if field == 'priority':
                    setattr(scheduled_job, field, JobPriority[value.value])
                else:
                    setattr(scheduled_job, field, value)

        # Recalculate next run if cron changed
        if update_data.cron_expression:
            now = datetime.now(timezone.utc)
            cron = croniter(update_data.cron_expression, now)
            scheduled_job.next_run_at = cron.get_next(datetime)

        await db.commit()
        await db.refresh(scheduled_job)

        print(f"✓ Updated scheduled job {schedule_id}")

        return scheduled_job

    @staticmethod
    async def delete_scheduled_job(
            db: AsyncSession,
            schedule_id: str
    ) -> bool:

        scheduled_job = await ScheduledJobService.get_scheduled_job(db, schedule_id)
        if not scheduled_job:
            return False

        await db.delete(scheduled_job)
        await db.commit()

        print(f"✓ Deleted scheduled job {schedule_id}")

        return True

    @staticmethod
    async def get_due_schedules(db: AsyncSession) -> List[ScheduledJob]:

        now = datetime.now(timezone.utc)

        result = await db.execute(
            select(ScheduledJob).where(
                and_(
                    ScheduledJob.is_active == True,
                    ScheduledJob.next_run_at <= now
                )
            )
        )

        return list(result.scalars().all())

    @staticmethod
    async def execute_schedule(
            db: AsyncSession,
            schedule_id: str
    ) -> bool:

        scheduled_job = await ScheduledJobService.get_scheduled_job(db, schedule_id)
        if not scheduled_job or not scheduled_job.is_active:
            return False

        # Create job instance
        job_data = JobCreate(
            name=scheduled_job.name,
            priority=scheduled_job.priority.value,
            payload=scheduled_job.payload,
            max_retries=scheduled_job.max_retries,
        )

        job = await JobService.create_job(
            db,
            job_data,
            scheduled_job_id=schedule_id
        )

        # Update last run time
        scheduled_job.last_run_at = datetime.now(timezone.utc)

        # Calculate next run time
        cron = croniter(
            scheduled_job.cron_expression,
            scheduled_job.last_run_at
        )
        scheduled_job.next_run_at = cron.get_next(datetime)

        await db.commit()

        print(f"✓ Executed scheduled job {schedule_id}")
        print(f"  Created job: {job.id}")
        print(f"  Next run: {scheduled_job.next_run_at}")

        return True

    @staticmethod
    def validate_cron_expression(expression: str) -> bool:

        return croniter.is_valid(expression)

    @staticmethod
    def get_next_run_times(
            expression: str,
            count: int = 5
    ) -> List[datetime]:

        now = datetime.now(timezone.utc)
        cron = croniter(expression, now)

        return [cron.get_next(datetime) for _ in range(count)]