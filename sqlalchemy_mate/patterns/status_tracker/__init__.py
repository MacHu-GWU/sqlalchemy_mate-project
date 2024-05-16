# -*- coding: utf-8 -*-

"""
Use Relational database to track the status of many jobs, with error handling,
retry, catch up, etc ... This pattern ensure that there is only one worker
can work on the specific job at a time.

This module is inspired by https://github.com/MacHu-GWU/pynamodb_mate-project/blob/master/examples/patterns/status-tracker.ipynb
"""
