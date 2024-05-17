# -*- coding: utf-8 -*-

import time
import random


from sqlalchemy_mate.tests.status_tracker_test import StatusEnum, Job
from sqlalchemy_mate.tests.api import engine_psql as engine


job_list = Job.query_by_status(engine, StatusEnum.pending.value, limit=5)
print(
    [
        dict(id=job.id, status=job.status, update_at=str(job.update_at))
        for job in job_list
    ]
)
for job in job_list:
    with Job.start_job(id=job.id, debug=True):
        time.sleep(random.randint(1, 10))
