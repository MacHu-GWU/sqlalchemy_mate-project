#!/bin/bash
# -*- coding: utf-8 -*-

docker run --rm --name sqlalchemy-mate-dev-psql -p 40311:5432 -e POSTGRES_PASSWORD=password -d postgres:10.6-alpine
sleep 1
