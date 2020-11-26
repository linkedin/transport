#!/usr/bin/env bash

# TravisCI calls this script to build and test the Transport code.
# Gradle commands that are specific to the release process are
# called directly from the Travis CI configuration file.
# The rationale for placing these commands in a separate script is
# to make it easier for contributors to run these checks before
# submitting a PR.

set -e

cd "$(dirname "$0")"

./gradlew clean build -s
./gradlew -p transportable-udfs-examples clean build -s
