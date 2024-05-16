# -*- coding: utf-8 -*-

import sqlalchemy as sa
import sqlalchemy.orm as orm

from sqlalchemy_mate.tests import engine_psql as engine


Base = orm.declarative_base()

class Account()