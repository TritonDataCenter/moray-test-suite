/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var child = require('child_process');
var forkexec = require('forkexec');
var fs = require('fs');
var jsprim = require('jsprim');
var mod_fast = require('fast');
var mod_net = require('net');
var mod_url = require('url');
var path = require('path');
var stream = require('stream');
var util = require('util');
var VError = require('verror');

var bunyan = require('bunyan');
var moray = require('moray'); // client

var which = 0;

///--- API

function createLogger(name, logstream) {
    var log = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'warn'),
        name: name || process.argv[1],
        stream: logstream || process.stdout,
        src: true,
        serializers: bunyan.stdSerializers
    });
    return (log);
}

function createClient(opts) {
    /*
     * It would be nice to use the mustCloseBeforeNormalProcessExit option to
     * the Moray client, which would identify client leaks, but node-tape
     * defeats that check by calling process.exit() from its own 'exit'
     * listener.
     */
    var clientparams = {};

    if (process.env['MORAY_TEST_SERVER_REMOTE']) {
        clientparams.url = process.env['MORAY_TEST_SERVER_REMOTE'];
    } else {
        clientparams.host = '127.0.0.1';
        clientparams.port = 2020;
    }

    clientparams.log = createLogger();

    if (opts && opts.unwrapErrors !== undefined) {
        clientparams.unwrapErrors = opts.unwrapErrors;
    }

    if (opts && opts.requireIndexes !== undefined) {
        clientparams.requireIndexes = opts.requireIndexes;
    }

    return (moray.createClient(clientparams));
}

/*
 * Make a Fast RPC to the Moray server.
 *
 * Normally tests would use the Moray client, but the client performs some
 * sanity checks, and enforces certain behaviour. This function allows for
 * testing how the Moray server handles bad parameters, and nonexistent
 * endpoints.
 *
 * Arguments:
 * - opts (Object):
 *   - log, a Bunyan logger
 *   - call, the Fast parameters to pass to rpcBufferAndCallback()
 * - cb (Function), callback that gets passed rpcBufferAndCallback's results
 */
function makeFastRequest(opts, cb) {
    assert.object(opts, 'opts');
    assert.func(cb, 'callback');

    var host, port;

    if (process.env['MORAY_TEST_SERVER_REMOTE']) {
        var parsed = mod_url.parse(process.env['MORAY_TEST_SERVER_REMOTE']);
        host = parsed.hostname;
        port = parsed.port;
    } else {
        host = '127.0.0.1';
        port = 2020;
    }

    var socket = mod_net.connect(port, host);

    socket.on('error', cb);

    socket.on('connect', function () {
        socket.removeListener('error', cb);

        var client = new mod_fast.FastClient({
            log: opts.log,
            nRecentRequests: 100,
            transport: socket
        });

        client.rpcBufferAndCallback(opts.call, function (err, data, ndata) {
            client.detach();
            socket.destroy();
            cb(err, data, ndata);
        });
    });
}

function multipleServersSupported() {
    return (!process.env['MORAY_TEST_SERVER_REMOTE']);
}

function createServer(opts, cb) {
    var env, cp, pt, server, t, seen, ready;

    opts = opts || {};
    if (process.env['MORAY_TEST_SERVER_REMOTE']) {
        if (opts.portOverride) {
            throw (new Error('multiple servers are not supported in ' +
                'this configuration'));
        } else {
            setImmediate(cb, { 'ts_remote': true });
        }
        return;
    }

    if (!process.env['MORAY_TEST_SERVER_RUN']) {
        throw (new Error('not found in environment: MORAY_TEST_SERVER_RUN. ' +
            '(have you already run configure and sourced the env file?)'));
    }

    env = jsprim.deepCopy(process.env);
    if (opts.portOverride) {
        env['MORAY_TEST_EXTRA_ARGS'] = '-p ' + opts.portOverride +
            ' -k ' + (opts.portOverride + 1000);
    }

    cp = child.spawn('bash', [ '-c', process.env['MORAY_TEST_SERVER_RUN'] ], {
            'detached': true,
            'stdio': [ 'ignore', 'pipe', process.stderr ],
            'env': env
        });

    seen = '';
    ready = false;
    pt = new stream.PassThrough();
    cp.stdout.pipe(process.stdout);
    cp.stdout.pipe(pt);

    pt.on('data', function (c) {
        seen += c.toString('utf8');
        if (!ready && /moray listening on .*\d+/i.test(seen)) {
            cp.stdout.unpipe(pt);
            ready = true;
            clearTimeout(t);
            t = null;
            cb(server);
        }
    });

    t = setTimeout(function () {
        throw (new Error('server did not start after 10 seconds'));
    }, 10000);

    server = {
        'ts_remote': false,
        'ts_child': cp,
        'ts_cleanup_cb': null
    };

    cp.on('exit', function (code, signal) {
        var err, info;

        if (code === 0) {
            /*
             * This should never happen because the server should only exit when
             * we kill it, and that won't be a clean exit.
             */
            throw (new Error('server unexpectedly exited with status 0'));
        }

        if (server.ts_cleanup_cb === null || signal != 'SIGKILL') {
            err = new Error('child process exited');
            err.code = code;
            err.signal = signal;

            info = forkexec.interpretChildProcessResult({
                'label': 'test moray server',
                'error': err
            });

            throw (info.error);
        } else {
            server.ts_cleanup_cb();
        }
    });
}

function cleanupServer(server, cb) {
    if (server.ts_remote) {
        setImmediate(cb);
    } else {
        assert.ok(server.ts_cleanup_cb === null,
            'cannot call cleanupServer multiple times');
        server.ts_cleanup_cb = cb;

        /*
         * Kill the entire process group, since there may have been more than
         * one process created under bash.
         */
        process.kill(-server.ts_child.pid, 'SIGKILL');
    }
}

/*
 * This function behaves like t.deepEqual(), except that it ignores properties
 * in "actual" that are not present in "expected".  This applies recursively, so
 * that if actual.x.y exists but expected.x.y doesn't (but actual.x and
 * expected.x are otherwise equivalent), then no error is thrown.
 *
 * Arguments:
 *
 *     t            the node-tape test context
 *
 *     expected     expected object
 *
 *     actual       actual object
 *
 *     prefix       property name for the top-level objects.  This is used to
 *                  construct specific error messages when subproperties don't
 *                  match.
 */
function checkDeepSubset(t, expected, actual, prefix) {
    var k;

    assert.object(t, 't');
    assert.object(expected, 'expected');
    assert.object(actual, 'actual');
    assert.string(prefix, 'prefix');

    for (k in expected) {
        if (typeof (expected[k]) == 'object' &&
            typeof (actual[k]) == 'object' &&
            expected[k] !== null && actual[k] !== null &&
            !Array.isArray(expected[k]) && !Array.isArray(actual[k])) {

            checkDeepSubset(t, expected[k], actual[k], prefix + '.' + k);
        } else {
            t.deepEqual(actual[k], expected[k], prefix + '.' + k + '  matches');
        }
    }
}

/*
 * Defines a node-tape test-case called "tc.name" for testing a stateless,
 * synchronous function "func" with specific input "tc.input".  If "tc.output"
 * is specified, the function should return an object of which "tc.output" is a
 * subset (according to checkDeepSubset()).  Otherwise, "tc.errmsg" must be
 * specified, and the function must thrown an exception such that t.throws(...,
 * errmsg) passes.
 */
function defineStatelessTestCase(tape, func, tc) {
    assert.string(tc.name);
    assert.object(tc.input);
    assert.optionalObject(tc.output);
    assert.ok(tc.output || tc.errmsg);
    assert.ok(!(tc.output && tc.errmsg));

    tape.test(tc.name, function runTestCase(t) {
        var rv;

        if (tc.errmsg) {
            t.throws(function () {
                func(tc.input);
            }, tc.errmsg);
        } else {
            rv = func(tc.input);
            assert.object(rv);
            checkDeepSubset(t, tc.output, rv, 'result');
        }

        t.end();
    });
}

function performFindObjectsTest(t, client, options) {
    assert.object(t, 't');
    assert.object(client, 'client');
    assert.object(options, 'options');
    assert.string(options.bucketName, 'options.bucketName');
    assert.string(options.searchFilter, 'options.searchFilter');
    assert.object(options.findObjectsOpts, 'options.findObjectsOpts');
    assert.object(options.expectedResults, 'options.expectedResults');
    assert.bool(options.expectedResults.error, 'options.expectedResults.error');
    assert.number(options.expectedResults.nbRecordsFound,
        'options.expectedResults.nbRecordsFound');
    assert.optionalString(options.expectedResults.errMsg,
        'options.expectedResults.errMsg');
    assert.optionalFunc(options.expectedResults.verifyRecords,
        'options.expectedResults.verifyRecords');

    var bucketName = options.bucketName;
    var errorExpected = options.expectedResults.error;
    var findObjectsOpts = jsprim.deepCopy(options.findObjectsOpts);
    var nbRecordsExpected = options.expectedResults.nbRecordsFound;
    var nbRecordsFound = 0;
    var req;
    var searchFilter = options.searchFilter;

    /*
     * A function that accepts an array or all the records
     * found during a test. It should return either true
     * or false, depending on whether the test should pass
     * or fail. This function is currently used to verify
     * the order of the records returned to a findobjects
     * query that requests a sort.
     */
    var verifyRecords = options.expectedResults.verifyRecords;
    var recordsReceived = [];

    /*
     * We intentionally bypass the bucket cache when performing findObjects
     * requests because we want to run tests before and after the test bucket
     * has been reindexed, and we don't want to wait for all buckets to have
     * their cache expired before we can be sure that all instances of the moray
     * service we're connected to have their bucket cache reflect the fact that
     * all indexes are usable.
     */
    findObjectsOpts.noBucketCache = true;

    req = client.findObjects(bucketName, searchFilter, findObjectsOpts);

    req.on('error', function onFindObjError(findObjErr) {
        var expectedErrorName = 'NotIndexedError';
        var expectedErrMsg = options.expectedResults.errMsg;

        if (errorExpected) {
            t.ok(findObjErr, 'findObjects request should error');
            t.ok(VError.hasCauseWithName(findObjErr, expectedErrorName),
                'error name should be ' + expectedErrorName);

            if (expectedErrMsg) {
                t.ok(findObjErr.message.indexOf(expectedErrMsg) !== -1,
                    'Error message should include: ' + expectedErrMsg);
            }

            t.equal(nbRecordsFound, 0,
                'no record should have been sent as part of the response');
        } else {
            t.ifErr(findObjErr, 'findObjects request should not error');
        }

        t.end();
    });

    req.on('record', function onRecord(record) {
        ++nbRecordsFound;
        recordsReceived.push(record);
    });

    req.on('end', function onFindObjEnd() {
        if (errorExpected) {
            t.fail('should not get end event, only error event');
        } else {
            t.pass('should get end event and not error');
            t.equal(nbRecordsFound, nbRecordsExpected, 'should have found ' +
                nbRecordsExpected + ' record');
            if (verifyRecords) {
                t.ok(verifyRecords(recordsReceived), 'verifyRecords failed');
            }
        }
        t.end();
    });
}


///--- Exports

module.exports = {
    makeFastRequest: makeFastRequest,
    multipleServersSupported: multipleServersSupported,
    createLogger: createLogger,
    createClient: createClient,
    createServer: createServer,
    checkDeepSubset: checkDeepSubset,
    cleanupServer: cleanupServer,
    defineStatelessTestCase: defineStatelessTestCase,
    performFindObjectsTest: performFindObjectsTest
};
