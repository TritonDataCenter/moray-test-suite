/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * ee_error.test.js: tests that RPC calls using event emitters that don't attach
 * an 'error' listener cause the program to crash when the RPC fails.
 */

var domain = require('domain');
var tape = require('tape');
var VError = require('verror');

var helper = require('./helper.js');
var server, client, timeout;

tape.test('findObjects error with no handler causes crash', function (t) {
    helper.createServer(null, function (s) {
        server = s;
        client = helper.createClient();
        client.on('connect', function () {
            /*
             * Use of domains is generally regrettable, but there's not another
             * great way for us to confirm that an uncaught exception was
             * thrown, short of replacing the surrounding test framework with
             * one capable of testing that the Node program itself has crashed.
             */
            var dom = domain.create();
            timeout = setTimeout(function () {
                t.fail('request timed out (expected failure)');
                t.end();
            }, 10000);

            dom.run(function () {
                var req = client.findObjects('a_bucket', 'bad_filter=)');
                req.on('end', function () {
                    t.fail('request succeeded (expected failure');
                    t.end();
                });
            });

            dom.on('error', function (err) {
                clearTimeout(timeout);
                t.ok(err);
                t.ok(err instanceof Error);
                t.ok(VError.hasCauseWithName(err, 'FastRequestError'));
                t.end();
            });
        });
    });
});

tape.test('cleanup', function (t) {
    client.close();
    helper.cleanupServer(server, function () {
        t.pass('closed');
        t.end();
    });
});
