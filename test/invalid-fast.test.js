/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var test = require('tape');
var vasync = require('vasync');
var VError = require('verror');

var helper = require('./helper.js');

var fmt = require('util').format;

///--- Globals

var LOG = helper.createLogger('invalid-moray-fast');

var BAD_RPCS = [

    // Invalid PutObjects
    {
        method: 'putObject',
        args: [ 'mybucket', 'mykey', [ 1, 2, 3 ], {} ],
        errname: 'InvocationError',
        errmsg: 'putObject expects "value" (args[2]) to be an object'
    },

    // Invalid DeleteObjects
    {
        method: 'delObject',
        args: [ 'mybucket', 3041, {} ],
        errname: 'InvocationError',
        errmsg: 'delObject expects "key" (args[1]) to be a nonempty string'
    },

    // Invalid GetObjects
    {
        method: 'getObject',
        args: [ true, 'mykey', {} ],
        errname: 'InvocationError',
        errmsg: 'getObject expects "bucket" (args[0]) to be a nonempty string'
    },

    // Invalid FindObjects
    {
        method: 'findObjects',
        args: [ 'mybucket', [ '(a=5)' ], {} ],
        errname: 'InvocationError',
        errmsg: 'findObjects expects "filter" (args[1]) to be a nonempty string'
    },

    // Invalid ReindexObjects
    {
        method: 'reindexObjects',
        args: [ 'mybucket', -20, {} ],
        errname: 'InvocationError',
        errmsg: 'reindexObjects expects "count" (args[1]) to be a nonnegative integer'
    },

    // Invalid DeleteMany
    {
        method: 'deleteMany',
        args: [ 'mybucket', '(a=5)', null ],
        errname: 'InvocationError',
        errmsg: 'deleteMany expects "options" (args[2]) to be an object'
    },

    // Invalid UpdateObjects
    {
        method: 'updateObjects',
        args: [ 'mybucket', 'a=5', '(a=6)', {} ],
        errname: 'InvocationError',
        errmsg: 'updateObjects expects "fields" (args[1]) to be an object'
    },

    // Invalid Batches
    {
        method: 'batch',
        args: [
            { operation: 'delete', bucket: 'mybucket', key: 'mykey' },
            {}
        ],
        errname: 'InvocationError',
        errmsg: 'batch expects "requests" (args[0]) to be an array'
    },

    // Invalid CreateBuckets
    {
        method: 'createBucket',
        args: [ true, { index: {} }, {} ],
        errname: 'InvocationError',
        errmsg: 'createBucket expects "bucket" (args[0]) to be a nonempty string'
    },

    // Invalid DeleteBuckets
    {
        method: 'delBucket',
        args: [ null, {} ],
        errname: 'InvocationError',
        errmsg: 'delBucket expects "bucket" (args[0]) to be a nonempty string'
    },

    // Invalid GetBuckets
    {
        method: 'getBucket',
        args: [ null, 'mybucket' ],
        errname: 'InvocationError',
        errmsg: 'getBucket expects "options" (args[0]) to be an object'
    },

    // Invalid UpdateBuckets
    {
        method: 'updateBucket',
        args: [ 'mybucket', false, {} ],
        errname: 'InvocationError',
        errmsg: 'updateBucket expects "config" (args[1]) to be an object'
    },

    // Invalid ListBuckets
    {
        method: 'listBuckets',
        args: [ [ ] ],
        errname: 'InvocationError',
        errmsg: 'listBuckets expects "options" (args[0]) to be an object'
    },

    // Invalid SQL
    {
        method: 'sql',
        args: [ 'SELECT now();', null, {} ],
        errname: 'InvocationError',
        errmsg: 'sql expects "values" (args[1]) to be an array'
    },

    // Invalid Pings
    {
        method: 'ping',
        args: [ null ],
        errname: 'InvocationError',
        errmsg: 'ping expects "options" (args[0]) to be an object'
    },

    // Invalid Versions
    {
        method: 'version',
        args: [ null ],
        errname: 'InvocationError',
        errmsg: 'version expects "options" (args[0]) to be an object'
    },

    // Invalid GetTokens
    {
        method: 'getTokens',
        args: [ null ],
        errname: 'InvocationError',
        errmsg: 'getTokens expects "options" (args[0]) to be an object'
    }
];

var RPC_ARG_COUNTS = [
    { method: 'createBucket', count: 3 },
    { method: 'getBucket', count: 2 },
    { method: 'listBuckets', count: 1 },
    { method: 'updateBucket', count: 3 },
    { method: 'delBucket', count: 2 },
    { method: 'putObject', count: 4 },
    { method: 'batch', count: 2 },
    { method: 'getObject', count: 3 },
    { method: 'delObject', count: 3 },
    { method: 'findObjects', count: 3 },
    { method: 'updateObjects', count: 4 },
    { method: 'reindexObjects', count: 3 },
    { method: 'deleteMany', count: 3 },
    { method: 'getTokens', count: 1 },
    { method: 'sql', count: 3 },
    { method: 'ping', count: 1 },
    { method: 'version', count: 1 }
];


///--- Tests

BAD_RPCS.forEach(function (cfg) {
    assert.string(cfg.errname, 'errname');
    assert.string(cfg.errmsg, 'errmsg');
    assert.string(cfg.method, 'method');
    assert.array(cfg.args, 'args');

    test(cfg.errname + ': ' + cfg.errmsg, function (t) {
        helper.makeFastRequest({
            log: LOG,
            call: {
                rpcmethod: cfg.method,
                rpcargs: cfg.args,
                maxObjectsToBuffer: 100
            }
        }, function (err, data, ndata) {
            t.ok(err, 'expected error');
            t.deepEqual([], data, 'expected no results');
            t.deepEqual(0, ndata, 'expected no results');
            if (err) {
                var cause = VError.findCauseByName(err, cfg.errname);
                t.ok(cause, 'expected a ' + cfg.errname);
                if (cause && cause.message.indexOf(cfg.errmsg) !== -1) {
                    t.pass('correct error message');
                } else {
                    t.equal(cfg.errmsg, cause.message, 'correct error message');
                }
            }
            t.end();
        });
    });
});


RPC_ARG_COUNTS.forEach(function (cfg) {
    assert.string(cfg.method, 'method');
    assert.number(cfg.count, 'count');

    var args = [];
    for (var i = 1; i < cfg.count; i++) {
        args.push(i);
    }

    var msg = fmt('%s expects %d argument%s',
        cfg.method, cfg.count, cfg.count === 1 ? '' : 's');

    test(cfg.method + ' expects rcpargs.length == ' + cfg.count, function (t) {
        helper.makeFastRequest({
            log: LOG,
            call: {
                rpcmethod: cfg.method,
                rpcargs: args,
                maxObjectsToBuffer: 100
            }
        }, function (err, data, ndata) {
            t.ok(err, 'expected error');
            t.deepEqual([], data, 'expected no results');
            t.deepEqual(0, ndata, 'expected no results');
            if (err) {
                var cause = VError.findCauseByName(err, 'InvocationError');
                t.ok(cause, 'expected a ' + cfg.errname);
                if (cause && cause.message.indexOf(cfg.errmsg) !== -1) {
                    t.pass('correct error message');
                } else {
                    t.equal(msg, cause.message, 'correct error message');
                }
            }
            t.end();
        });
    });
});
