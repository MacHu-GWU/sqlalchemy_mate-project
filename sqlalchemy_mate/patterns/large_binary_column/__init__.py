# -*- coding: utf-8 -*-

"""
The large binary column pattern is a technique designed to optimize
storage and performance when dealing with sizable binary data,
typically exceeding 1KB. Instead of directly storing the binary data in the
relational database, this approach involves saving the data in
a dedicated storage layer and keeping only a reference to its location,
in the form of a unique resource identifier (URI), within the database.
By adopting this pattern, valuable database disk space and I/O resources
can be conserved.

This module provides an implementation of the large binary column pattern,
offering flexibility in terms of storage backends. Users have the option to
utilize various storage solutions, such as the file system or Amazon S3,
 depending on their specific requirements. Furthermore, the module is designed
 with extensibility in mind, allowing users to implement additional
 storage backends to suit their needs.
"""
