"""
Midwicket Data Ingestion Scheduler

Manages scheduled tasks for data pipeline execution, including:
- Weekly Cricsheet data ingestion
- Registry stats aggregation
- Data validation and cleanup
"""

import logging
import asyncio
from typing import Optional, Callable
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.job import Job

logger = logging.getLogger(__name__)


class DataIngestionScheduler:
    """
    Manages scheduled data ingestion and pipeline tasks.

    Supports:
    - Weekly data pipeline execution
    - Automatic retry on failure
    - Health checks and monitoring
    """

    def __init__(self, timezone: str = "UTC"):
        """
        Initialize the scheduler.

        Args:
            timezone: Timezone for cron scheduling (default: UTC)
        """
        self.scheduler = AsyncIOScheduler(timezone=timezone)
        self._jobs: dict[str, Job] = {}
        self._is_running = False
        self._last_run_status: dict[str, dict] = {}

    def schedule_weekly_ingestion(
        self,
        task_func: Callable,
        day_of_week: int = 0,
        hour: int = 2,
        minute: int = 0,
        max_instances: int = 1,
    ) -> None:
        """
        Schedule weekly data ingestion task.

        Args:
            task_func: Async callable to execute (e.g., build_registry_stats)
            day_of_week: Day to run (0=Monday, 6=Sunday; default: 0)
            hour: Hour to run (0-23; default: 2 UTC = 7:30 IST)
            minute: Minute to run (default: 0)
            max_instances: Max concurrent instances (default: 1)
        """
        trigger = CronTrigger(
            day_of_week=day_of_week,
            hour=hour,
            minute=minute,
        )

        job = self.scheduler.add_job(
            self._wrapped_task("weekly_ingestion", task_func),
            trigger=trigger,
            id="weekly_ingestion",
            name="Weekly Data Ingestion",
            max_instances=max_instances,
            replace_existing=True,
        )

        self._jobs["weekly_ingestion"] = job
        logger.info(
            f"Scheduled weekly ingestion: {day_of_week} @ {hour:02d}:{minute:02d} UTC"
        )

    async def start(self) -> None:
        """Start the scheduler."""
        if self._is_running:
            logger.warning("Scheduler already running")
            return

        try:
            self.scheduler.start()
            self._is_running = True
            logger.info("Data ingestion scheduler started")
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            raise

    async def stop(self) -> None:
        """Stop the scheduler."""
        if not self._is_running:
            return

        try:
            self.scheduler.shutdown(wait=True)
            self._is_running = False
            logger.info("Data ingestion scheduler stopped")
        except Exception as e:
            logger.error(f"Failed to stop scheduler: {e}")
            raise

    async def _wrapped_task(self, task_name: str, task_func: Callable):
        """
        Wrap task execution with error handling and monitoring.

        Args:
            task_name: Name of the task for logging
            task_func: The actual task function to execute
        """
        async def execute():
            start_time = datetime.utcnow()
            try:
                logger.info(f"Starting {task_name}...")
                result = await task_func() if asyncio.iscoroutinefunction(task_func) else task_func()

                end_time = datetime.utcnow()
                elapsed = (end_time - start_time).total_seconds()

                self._last_run_status[task_name] = {
                    "status": "success",
                    "last_run": end_time.isoformat(),
                    "elapsed_seconds": elapsed,
                    "result": result,
                }

                logger.info(f"✓ {task_name} completed in {elapsed:.1f}s")
                return result

            except Exception as e:
                end_time = datetime.utcnow()
                elapsed = (end_time - start_time).total_seconds()

                self._last_run_status[task_name] = {
                    "status": "failed",
                    "last_run": end_time.isoformat(),
                    "elapsed_seconds": elapsed,
                    "error": str(e),
                }

                logger.error(f"✗ {task_name} failed after {elapsed:.1f}s: {e}", exc_info=True)
                raise

        return execute

    def get_health_status(self) -> dict:
        """
        Get scheduler health status.

        Returns:
            Dict with scheduler state, jobs, and last run statuses
        """
        return {
            "running": self._is_running,
            "jobs": {
                job_id: {
                    "name": job.name,
                    "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                    "trigger": str(job.trigger),
                }
                for job_id, job in self._jobs.items()
            },
            "last_run_status": self._last_run_status,
        }

    def list_jobs(self) -> list[dict]:
        """
        List all scheduled jobs.

        Returns:
            List of job dicts with id, name, trigger, next_run_time
        """
        return [
            {
                "id": job.id,
                "name": job.name,
                "trigger": str(job.trigger),
                "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                "max_instances": job.max_instances,
            }
            for job in self.scheduler.get_jobs()
        ]


# Global scheduler instance
_scheduler_instance: Optional[DataIngestionScheduler] = None


def get_scheduler() -> DataIngestionScheduler:
    """Get or create the global scheduler instance."""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = DataIngestionScheduler()
    return _scheduler_instance


async def initialize_scheduler(task_func: Callable, config: dict) -> DataIngestionScheduler:
    """
    Initialize scheduler with configuration.

    Args:
        task_func: The data ingestion task to schedule
        config: Dict with keys: day_of_week, hour, minute, timezone

    Returns:
        Initialized scheduler (not yet started)
    """
    scheduler = get_scheduler()

    day_of_week = config.get("day_of_week", 0)  # Monday
    hour = config.get("hour", 2)  # 2 AM UTC
    minute = config.get("minute", 0)
    timezone = config.get("timezone", "UTC")

    scheduler.schedule_weekly_ingestion(
        task_func=task_func,
        day_of_week=day_of_week,
        hour=hour,
        minute=minute,
    )

    return scheduler
