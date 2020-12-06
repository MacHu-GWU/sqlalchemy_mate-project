#!/bin/bash
# -*- coding: utf-8 -*-

docker run --rm --name test-psql-db -p 43347:5432 -e POSTGRES_PASSWORD=password -d postgres:10.6-alpine
sleep 1
