#!/bin/sh
# Unit tests for the new HTTP client features:
#   1. SimpleAsyncHTTPClient keep-alive connection pool
#      (keyed by host/port/is_ssl, max_idle_connections, idle_timeout,
#       dead-connection eviction via IOStream.set_close_callback)
#   2. AsyncHTTPClient request-level retries with exponential backoff + jitter
#   3. HTTPResponse body streaming truncation (truncated=True)
#
# Usage: ./test_httpclient_features.sh
cd "$(dirname "$0")"

PYTHON=${PYTHON:-python3}

set -e

echo "=== New feature tests ==="
$PYTHON -m tornado.test.runtests \
    tornado.test.httpclient_test.RequestRetryTestCase \
    tornado.test.simple_httpclient_test.HTTPConnectionPoolUnitTestCase \
    tornado.test.simple_httpclient_test.ConnectionPoolTestCase \
    tornado.test.simple_httpclient_test.BodyTruncationTestCase

echo "=== Full httpclient regression suites ==="
$PYTHON -m tornado.test.runtests \
    tornado.test.httpclient_test \
    tornado.test.simple_httpclient_test
