# -*- coding: utf-8 -*-

"""
``test_status_tracking_1.py`` and ``test_status_tracking_2.py`` are designed to
simulate a situation where two worker are trying to update the same job.

Each job in ``test_status_tracking_1.py`` will take 5-10 seconds to finish, but
each job in ``test_status_tracking_2.py`` will finishe immediately. If you run
``test_status_tracking_1.py`` first then run ``test_status_tracking_2.py``
immediately, you will see the #1 worker failed at job 2 because the #2 worker
finished job 2 immediately.
"""

import time
import random

import sqlalchemy.orm as orm

from sqlalchemy_mate.tests.status_tracker_test import StatusEnum, Job
from sqlalchemy_mate.tests.api import engine_psql as engine


with orm.Session(engine) as ses:
    ses.execute(Job.__table__.delete())
    ses.commit()

    for i in range(1, 20):
        job = Job.create(
            id=f"job-{i}", status=StatusEnum.pending.value, data={"version": 1}
        )
        ses.add(job)
    ses.commit()


job_list = Job.query_by_status(engine, StatusEnum.pending.value, limit=5)
print(
    [
        dict(id=job.id, status=job.status, update_at=str(job.update_at))
        for job in job_list
    ]
)
for job in job_list:
    with Job.start_job(id=job.id, debug=True):
        time.sleep(random.randint(5, 10))
