#!/bin/bash
# -*- coding: utf-8 -*-
#
# This script should be sourced to use.


# GitHub
github_account="MacHu-GWU"
github_repo_name="sqlalchemy_mate-project"


# Python
package_name="sqlalchemy_mate"
py_ver_major="3"
py_ver_minor="6"
py_ver_micro="2"
use_pyenv="Y" # "Y" or "N"
supported_py_versions="2.7.13 3.4.6 3.5.3 3.6.2" # e.g: "2.7.13 3.6.2"


#--- Doc Build

rtd_project_name="sqlalchemy_mate"

# AWS profile name for hosting doc on S3
# should be defined in ~/.aws/credentials
# read https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html for more information
aws_profile_doc_host="an-aws-profile-name"

# html doc will be upload to:
# "s3://${S3_BUCKET_DOC_HOST}/docs/${PACKAGE_NAME}/${PACKAGE_VERSION}"
s3_bucket_doc_host="a-s3-bucket-name"


