/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2017, Joyent, Inc.
 */

/*
 * These tests are for testing Moray RPCs with invalid values. Many of these
 * cannot actually be performed by a Moray client since the client prevents
 * passing bad parameters to some of the endpoints, so we perform the Fast
 * call directly, instead of going through the client.
 *
 * (Some of these calls could actually be tested using the Moray client, but
 * it would be within reason for the client to start checking those itself,
 * and we really want to test the server's behaviour here, not the client's.)
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

    // Invalid RPC methods
    {
        method: 'bogusName',
        args: [ ],
        errname: 'FastError',
        errmsg: 'unsupported RPC method: "bogusName"'
    },

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
    {
        method: 'findObjects',
        args: [ 'mybucket', '(a=5)', { sort: 5 } ],
        errname: 'InvocationError',
        errmsg: 'findObjects expects "options" (args[2]) to be ' +
            'a valid options object: options.sort should be'
    },
    {
        method: 'findObjects',
        args: [
            'mybucket',
            '(a=5)',
            { sort: { attribute: 'foo', order: 'bad' } }
        ],
        errname: 'InvocationError',
        errmsg: 'options.sort.order should be equal to ' +
            'one of the allowed values ("ASC", "DESC")'
    },

    // Invalid ReindexObjects
    {
        method: 'reindexObjects',
        args: [ 'mybucket', -20, {} ],
        errname: 'InvocationError',
        errmsg: 'reindexObjects expects "count" (args[1]) to be ' +
            'a nonnegative integer'
    },

    // Invalid DeleteMany
    {
        method: 'deleteMany',
        args: [ 'mybucket', '(a=5)', null ],
        errname: 'InvocationError',
        errmsg: 'deleteMany expects "options" (args[2]) to be ' +
            'a valid options object'
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
    {
        method: 'batch',
        args: [
            [ { operation: 'delete' } ],
            {}
        ],
        errname: 'InvocationError',
        errmsg: 'batch expects "requests" (args[0]) to be an array of ' +
            'valid request objects: requests[0] should have ' +
            'required property \'bucket\''
    },
    {
        method: 'batch',
        args: [
            [
                { operation: 'put', bucket: 'mybucket', key: 'mykey' },
                { operation: 'invalid', bucket: 'mybucket' }
            ],
            {}
        ],
        errname: 'InvocationError',
        errmsg: 'batch expects "requests" (args[0]) to be an array of ' +
            'valid request objects: requests[1].operation should be ' +
            'equal to one of the allowed values'
    },

    // Invalid CreateBuckets
    {
        method: 'createBucket',
        args: [ true, { index: {} }, {} ],
        errname: 'InvocationError',
        errmsg: 'createBucket expects "bucket" (args[0]) to be ' +
            'a nonempty string'
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
        errmsg: 'getBucket expects "options" (args[0]) to be ' +
            'a valid options object'
    },

    // Invalid UpdateBuckets
    {
        method: 'updateBucket',
        args: [ 'mybucket', false, {} ],
        errname: 'InvocationError',
        errmsg: 'updateBucket expects "config" (args[1]) to be an object'
    },
    {
        method: 'updateBucket',
        args: [ 'mybucket', {}, { no_reindex: 1 } ],
        errname: 'InvocationError',
        errmsg: 'updateBucket expects "options" (args[2]) to be ' +
            'a valid options object: options.no_reindex should be boolean'
    },

    // Invalid ListBuckets
    {
        method: 'listBuckets',
        args: [ [ ] ],
        errname: 'InvocationError',
        errmsg: 'listBuckets expects "options" (args[0]) to be ' +
            'a valid options object'
    },

    // Invalid SQL
    {
        method: 'sql',
        args: [ 'SELECT now();', null, {} ],
        errname: 'InvocationError',
        errmsg: 'sql expects "values" (args[1]) to be an array'
    },
    {
        method: 'sql',
        args: [ 'SELECT now();', [], { req_id: 'foo' } ],
        errname: 'InvocationError',
        errmsg: 'sql expects "options" (args[2]) to be ' +
            'a valid options object: options.req_id should match format "uuid"'
    },
    {
        method: 'sql',
        args: [ 'SELECT now();', [], { timeout: -1 } ],
        errname: 'InvocationError',
        errmsg: 'sql expects "options" (args[2]) to be ' +
            'a valid options object: options.timeout should be >= 0'
    },


    // Invalid Pings
    {
        method: 'ping',
        args: [ null ],
        errname: 'InvocationError',
        errmsg: 'ping expects "options" (args[0]) to be ' +
            'a valid options object: options should be object'
    },
    {
        method: 'ping',
        args: [ [] ],
        errname: 'InvocationError',
        errmsg: 'ping expects "options" (args[0]) to be ' +
            'a valid options object: options should be object'
    },
    {
        method: 'ping',
        args: [ { deep: 1 } ],
        errname: 'InvocationError',
        errmsg: 'ping expects "options" (args[0]) to be ' +
            'a valid options object: options.deep should be boolean'
    },

    // Invalid Versions
    {
        method: 'version',
        args: [ null ],
        errname: 'InvocationError',
        errmsg: 'version expects "options" (args[0]) to be ' +
           'a valid options object'
    },

    // Invalid GetTokens
    {
        method: 'getTokens',
        args: [ null ],
        errname: 'InvocationError',
        errmsg: 'getTokens expects "options" (args[0]) to be ' +
            'a valid options object'
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
            var cause;

            t.ok(err, 'expected error');
            t.deepEqual([], data, 'expected no results');
            t.deepEqual(0, ndata, 'expected no results');

            if (err) {
                cause = VError.findCauseByName(err, cfg.errname);
                t.ok(cause, 'expected a ' + cfg.errname);
                if (cause && cause.message.indexOf(cfg.errmsg) !== -1) {
                    t.pass('correct error message');
                } else {
                    t.equal(err.message, cfg.errmsg, 'correct error message');
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
            var cause;

            t.ok(err, 'expected error');
            t.deepEqual([], data, 'expected no results');
            t.deepEqual(0, ndata, 'expected no results');

            if (err) {
                cause = VError.findCauseByName(err, 'InvocationError');
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
