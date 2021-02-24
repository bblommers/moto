#!/bin/sh -l

echo "Hello $1"
time=$(date)
pwd
ls -la /github/workspace
# Verify that our version does not yet exist
# Verify that the Changelog contains our version
# Update moto/__init__.py to set the version

echo "::set-output name=time::$time"
