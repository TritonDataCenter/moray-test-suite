/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * cli-parsing.test.js: tests library interfaces for parsing options
 */

var assert = require('assert-plus');
var helper = require('./helper');
var moraycli = require('moray/lib/cmd');
var tape = require('tape');
var stream = require('stream');

var parseCliOptions = moraycli.parseCliOptions;
var argvbase = [ 'node', 'testcmd' ];

function parseOptions(args, callback) {
    var errstream, clientOptions, argv, parser, errdata;
    var gotusage = false;

    assert.object(args, 'args');
    assert.object(args.cliargs, 'args.cliargs');
    assert.object(args.env, 'args.env');

    errstream = new stream.PassThrough();
    clientOptions = {};
    argv = argvbase.concat(args.cliargs);
    parser = parseCliOptions({
        'argv': argv,
        'env': args.env,
        'errstream': errstream,
        'extraOptStr': '',
        'clientOptions': clientOptions,
        'onUsage': function () {
            gotusage = true;
        }
    });

    errdata = '';
    errstream.on('data', function (d) {
        errdata += d.toString('utf8');
    });

    errstream.on('end', function () {
        var lines = errdata.split('\n');
        assert.strictEqual(lines[lines.length - 1], '');
        lines.pop();

        callback({
            'argvRest': argv.slice(parser.optind()),
            'clientOptions': clientOptions,
            'errors': lines,
            'gotUsage': gotusage
        });
    });

    errstream.end();
}

tape.test('no environment, no arguments (all defaults)', function (t) {
    parseOptions({
        'cliargs': [],
        'env': {}
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': '127.0.0.1',
            'port': 2020,
            'srvDomain': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('no environment, -S specified', function (t) {
    parseOptions({
        'cliargs': [ '-S', 'snpp.net' ],
        'env': {}
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': undefined,
            'port': undefined,
            'srvDomain': 'snpp.net',
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('no environment, -h specified', function (t) {
    parseOptions({
        'cliargs': [ '-h', 'shelbyville.net' ],
        'env': {}
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': 'shelbyville.net',
            'port': 2020,
            'srvDomain': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('no environment, -p specified', function (t) {
    parseOptions({
        'cliargs': [ '-p', '2031' ],
        'env': {}
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': '127.0.0.1',
            'port': 2031,
            'srvDomain': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('no environment, --host and --port specified', function (t) {
    parseOptions({
        'cliargs': [ '--port', '2031', '--host', '10.1.2.3' ],
        'env': {}
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': '10.1.2.3',
            'port': 2031,
            'srvDomain': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('MORAY_SERVICE in environment', function (t) {
    parseOptions({
        'cliargs': [],
        'env': {
            'MORAY_SERVICE': 'snpp.net'
        }
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': undefined,
            'port': undefined,
            'srvDomain': 'snpp.net',
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('MORAY_SERVICE in environment, overridden with -h', function (t) {
    parseOptions({
        'cliargs': [ '-h', 'shelbyville.net' ],
        'env': {
            'MORAY_SERVICE': 'snpp.net'
        }
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': 'shelbyville.net',
            'port': 2020,
            'srvDomain': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('MORAY_SERVICE in environment, overridden with -p', function (t) {
    parseOptions({
        'cliargs': [ '-p', '2040' ],
        'env': {
            'MORAY_SERVICE': 'snpp.net'
        }
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': '127.0.0.1',
            'port': 2040,
            'srvDomain': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('MORAY_SERVICE in environment, overridden with --host and --port',
    function (t) {
    parseOptions({
        'cliargs': [ '--host', 'shelbyville.net', '--port', '2040' ],
        'env': {
            'MORAY_SERVICE': 'snpp.net'
        }
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': 'shelbyville.net',
            'port': 2040,
            'srvDomain': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('MORAY_SERVICE in environment, overridden with --service',
    function (t) {
    parseOptions({
        'cliargs': [ '--service', 'shelbyville.net' ],
        'env': {
            'MORAY_SERVICE': 'shelbyville.net'
        }
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'srvDomain': 'shelbyville.net',
            'host': undefined,
            'port': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('MORAY_SERVICE and MORAY_URL in environment, no args',
    function (t) {
    parseOptions({
        'cliargs': [],
        'env': {
            'MORAY_SERVICE': 'snpp.net',
            'MORAY_URL': 'tcp://shelbyville.net:1234'
        }
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'srvDomain': 'snpp.net',
            'host': undefined,
            'port': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('MORAY_SERVICE and MORAY_URL in environment, with -S',
    function (t) {
    parseOptions({
        'cliargs': [ '-S', 'ogdenville.net' ],
        'env': {
            'MORAY_SERVICE': 'snpp.net',
            'MORAY_URL': 'tcp://shelbyville.net:1234'
        }
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'srvDomain': 'ogdenville.net',
            'host': undefined,
            'port': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('MORAY_SERVICE and MORAY_URL in environment, with -p',
    function (t) {
    parseOptions({
        'cliargs': [ '-p', '3020' ],
        'env': {
            'MORAY_SERVICE': 'snpp.net',
            'MORAY_URL': 'tcp://shelbyville.net:1234'
        }
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'srvDomain': undefined,
            'host': 'shelbyville.net',
            'port': 3020,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('MORAY_URL in environment (host only)', function (t) {
    parseOptions({
        'cliargs': [],
        'env': {
            'MORAY_URL': 'tcp://shelbyville.net'
        }
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': 'shelbyville.net',
            'port': 2020,
            'srvDomain': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('MORAY_URL in environment (host and port)', function (t) {
    parseOptions({
        'cliargs': [],
        'env': {
            'MORAY_URL': 'tcp://shelbyville.net:1234'
        }
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': 'shelbyville.net',
            'port': 1234,
            'srvDomain': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('MORAY_URL in environment, -S specified', function (t) {
    parseOptions({
        'cliargs': [ '-S', 'snpp.net' ],
        'env': {
            'MORAY_URL': 'tcp://shelbyville.net:1234'
        }
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': undefined,
            'port': undefined,
            'srvDomain': 'snpp.net',
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('MORAY_URL in environment, -h specified', function (t) {
    parseOptions({
        'cliargs': [ '-h', 'snpp.net' ],
        'env': {
            'MORAY_URL': 'tcp://shelbyville.net:1234'
        }
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': 'snpp.net',
            'port': 1234,
            'srvDomain': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('MORAY_URL in environment, -p specified', function (t) {
    parseOptions({
        'cliargs': [ '-p', '3040' ],
        'env': {
            'MORAY_URL': 'tcp://shelbyville.net:1234'
        }
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': 'shelbyville.net',
            'port': 3040,
            'srvDomain': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

tape.test('MORAY_URL in environment, -h and -p specified', function (t) {
    parseOptions({
        'cliargs': [ '-h', '10.1.2.3', '-p', '3040' ],
        'env': {
            'MORAY_URL': 'tcp://shelbyville.net:1234'
        }
    }, function (result) {
        t.deepEqual(result.errors, []);
        t.deepEqual(result.argvRest, []);
        helper.checkDeepSubset(t, {
            'host': '10.1.2.3',
            'port': 3040,
            'srvDomain': undefined,
            'failFast': true
        }, result.clientOptions, 'clientOptions');
        t.ok(!result.gotUsage);
        t.end();
    });
});

/*
 * Failure cases
 */

/*
 * Note that there's no analog test for a bad port specified in MORAY_URL
 * because Node's implementation of url.parse() doesn't really allow us to
 * handle it well.
 */
tape.test('bad input: no environment, bad port specified with -p',
    function (t) {
    parseOptions({
        'cliargs': [ '-p', 'asdf' ],
        'env': {}
    }, function (result) {
        t.deepEqual(result.errors, [ '-p/--port: expected valid TCP port' ]);
        t.deepEqual(result.argvRest, []);
        t.ok(result.gotUsage);
        t.end();
    });
});

tape.test('bad input: -p specified with -S', function (t) {
    parseOptions({
        'cliargs': [ '-p', '201', '-S', 'junk' ],
        'env': {}
    }, function (result) {
        t.deepEqual(result.errors, [
            '-S/--service cannot be combined with -h/--host or -p/--port'
        ]);
        t.deepEqual(result.argvRest, []);
        t.ok(result.gotUsage);
        t.end();
    });
});

tape.test('bad input: -h specified with -S', function (t) {
    parseOptions({
        'cliargs': [ '-h', '1.2.3.4', '-S', 'junk' ],
        'env': {}
    }, function (result) {
        t.deepEqual(result.errors, [
            '-S/--service cannot be combined with -h/--host or -p/--port'
        ]);
        t.deepEqual(result.argvRest, []);
        t.ok(result.gotUsage);
        t.end();
    });
});

tape.test('bad input: -S specified with IP address', function (t) {
    parseOptions({
        'cliargs': [ '-S', '1.2.3.4' ],
        'env': {}
    }, function (result) {
        t.deepEqual(result.errors, [
            'cannot use an IP address with -S/--service/MORAY_SERVICE'
        ]);
        t.deepEqual(result.argvRest, []);
        t.ok(result.gotUsage);
        t.end();
    });
});

tape.test('bad input: -S specified with IP address', function (t) {
    parseOptions({
        'cliargs': [],
        'env': { 'MORAY_SERVICE': '1.2.3.4' }
    }, function (result) {
        t.deepEqual(result.errors, [
            'cannot use an IP address with -S/--service/MORAY_SERVICE'
        ]);
        t.deepEqual(result.argvRest, []);
        t.ok(result.gotUsage);
        t.end();
    });
});
