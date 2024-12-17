from datetime import datetime, timedelta
import os
import signal
import time
from threading import Thread

from rq import Queue
from rq.job import Job
from rq_scheduler import Scheduler
from rq_scheduler.utils import to_unix, from_unix

from tests import RQTestCase


def say_hello(name=None):
    """A job with a single argument and a return value."""
    if name is None:
        name = 'Stranger'
    return 'Hi there, %s!' % (name,)


class TestRRuleScheduler(RQTestCase):

    def setUp(self):
        super(TestRRuleScheduler, self).setUp()
        self.scheduler = Scheduler(connection=self.testconn)

    def test_rrule_persisted_correctly(self):
        """
        Ensure that rrule string gets correctly saved in Redis.
        """
        # Create a job that runs daily at 10:00 AM
        rrule_string = "FREQ=DAILY;BYHOUR=10;BYMINUTE=0"
        job = self.scheduler.rrule(rrule_string, say_hello)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job_from_queue.meta['rrule_string'], rrule_string)

        # Get the scheduled_time and convert it to a datetime object
        unix_time = self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id)
        datetime_time = from_unix(unix_time)

        # Check that hour=10, minute=0, seconds=0
        self.assertEqual(datetime_time.hour, 10)
        self.assertEqual(datetime_time.minute, 0)
        self.assertEqual(datetime_time.second, 0)

    def test_rrule_sets_timeout(self):
        """
        Ensure that a job scheduled via rrule can be created with
        a custom timeout.
        """
        timeout = 13
        job = self.scheduler.rrule("FREQ=DAILY", say_hello, timeout=timeout)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job_from_queue.timeout, timeout)

    def test_rrule_sets_id(self):
        """
        Ensure that a job scheduled via rrule can be created with
        a custom id
        """
        job_id = "hello-job-id"
        job = self.scheduler.rrule("FREQ=DAILY", say_hello, id=job_id)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job_id, job_from_queue.id)

    def test_rrule_sets_default_result_ttl(self):
        """
        Ensure that a job scheduled via rrule gets proper default
        result_ttl (-1) for periodic tasks.
        """
        job = self.scheduler.rrule("FREQ=DAILY", say_hello)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(-1, job_from_queue.result_ttl)

    def test_rrule_sets_description(self):
        """
        Ensure that a job scheduled via rrule can be created with
        a custom description
        """
        description = 'test description'
        job = self.scheduler.rrule("FREQ=DAILY", say_hello, description=description)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(description, job_from_queue.description)

    def test_rrule_job_gets_rescheduled(self):
        """
        Ensure jobs with rrule attribute are put back in the scheduler
        with the next occurrence time
        """
        # Create a job that runs every hour
        rrule_string = "FREQ=HOURLY"
        job = self.scheduler.rrule(rrule_string, say_hello)
        
        # Get current scheduled time
        old_next_scheduled_time = self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id)
        
        # Enqueue the job
        self.scheduler.enqueue_job(job)
        
        # Check that job was rescheduled
        self.assertIn(job.id, self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, -1))
        new_next_scheduled_time = self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id)
        
        # New scheduled time should be about 1 hour after the old one
        self.assertGreater(new_next_scheduled_time, old_next_scheduled_time)
        self.assertAlmostEqual(new_next_scheduled_time - old_next_scheduled_time, 3600, delta=10)

    def test_rrule_with_count(self):
        """
        Ensure jobs with rrule COUNT parameter stop after the specified number of occurrences
        """
        # Create a job that runs 3 times, every minute
        rrule_string = "FREQ=MINUTELY;COUNT=3"
        job = self.scheduler.rrule(rrule_string, say_hello)
        
        # Run the job 3 times
        for _ in range(3):
            self.scheduler.enqueue_job(job)
            self.assertIn(job.id, self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, -1))
        
        # Fourth run should not reschedule the job
        self.scheduler.enqueue_job(job)
        self.assertNotIn(job.id, self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, -1))

    def test_rrule_with_until(self):
        """
        Ensure jobs with rrule UNTIL parameter stop after the specified date
        """
        # Create a job that runs every minute until 5 minutes from now
        until_time = datetime.utcnow() + timedelta(minutes=5)
        rrule_string = f"FREQ=MINUTELY;UNTIL={until_time.strftime('%Y%m%dT%H%M%SZ')}"
        job = self.scheduler.rrule(rrule_string, say_hello)
        
        # First run should reschedule
        self.scheduler.enqueue_job(job)
        self.assertIn(job.id, self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, -1))
        
        # Manually set next run after UNTIL time
        self.testconn.zadd(self.scheduler.scheduled_jobs_key, {job.id: to_unix(until_time + timedelta(minutes=1))})
        
        # This run should not reschedule the job since it's past UNTIL time
        self.scheduler.enqueue_job(job)
        self.assertNotIn(job.id, self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, -1))

    def test_invalid_rrule(self):
        """
        Ensure that invalid rrule strings raise ValueError
        """
        with self.assertRaises(ValueError):
            self.scheduler.rrule("INVALID RRULE", say_hello)