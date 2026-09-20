#!/bin/sh
# Run the unit tests for the new httpclient features:
#   - SimpleAsyncHTTPClient keep-alive connection pool
#     (ConnectionPoolTestCase, ConnectionPoolUnitTestCase)
#   - AsyncHTTPClient automatic retry with backoff+jitter (RetryTestCase)
#   - HTTPResponse body truncation (BodyTruncationTestCase)
#
# Usage:
#   ./test_httpclient_features.sh                 # run all feature tests
#   ./test_httpclient_features.sh <test-name>...  # run specific tests
#
# To also run the pre-existing httpclient suites (requires network
# access for bind()/DNS):
#   ./test_httpclient_features.sh \
#       tornado.test.simple_httpclient_test tornado.test.httpclient_test

cd "$(dirname "$0")"

if [ $# -eq 0 ]; then
    set -- \
        tornado.test.simple_httpclient_test.ConnectionPoolTestCase \
        tornado.test.simple_httpclient_test.ConnectionPoolUnitTestCase \
        tornado.test.simple_httpclient_test.RetryTestCase \
        tornado.test.simple_httpclient_test.BodyTruncationTestCase
fi

exec python -m tornado.test.runtests "$@"
