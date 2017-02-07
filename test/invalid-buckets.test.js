/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2017, Joyent, Inc.
 */

/*
 * This file contains tests for invalid bucket configurations. The Moray
 * client enforces certain fields and fills in falsey ones, so we use the
 * Fast client to perform the RPCs directly here. (The client is also
 * within its rights to further enforce some of them in the future, too.)
 */

var assert = require('assert-plus');
var jsprim = require('jsprim');
var test = require('tape');
var vasync = require('vasync');
var VError = require('verror');

var helper = require('./helper.js');

var fmt = require('util').format;

///--- Globals

var LOG = helper.createLogger('invalid-moray-fast');

var str64 = '';
for (var i = 0; i < 64; i++) {
    str64 += 'a';
}


var VALID_BUCKET = {
    index: { },
    options: { version: 1 }
};

var DEFAULT_BUCKET = {
    name: 'mybucket',
    config: VALID_BUCKET
};

var BAD_BUCKETS = [
    // Bad bucket names
    {
        name: 'moray',
        errname: 'InvalidBucketNameError',
        errmsg: 'is not a valid bucket name'
    },
    {
        name: 'search',
        errname: 'InvalidBucketNameError',
        errmsg: 'is not a valid bucket name'
    },
    {
        name: 'buckets_config',
        errname: 'InvalidBucketNameError',
        errmsg: 'is not a valid bucket name'
    },
    {
        name: '_name',
        errname: 'InvalidBucketNameError',
        errmsg: 'is not a valid bucket name'
    },
    {
        name: 'a-b',
        errname: 'InvalidBucketNameError',
        errmsg: 'is not a valid bucket name'
    },
    {
        name: '1b',
        errname: 'InvalidBucketNameError',
        errmsg: 'is not a valid bucket name'
    },
    {
        name: str64,
        errname: 'InvalidBucketNameError',
        errmsg: 'is not a valid bucket name'
    },
    {
        name: '',
        errname: 'InvocationError',
        errmsg: 'createBucket expects "bucket" (args[0]) to be ' +
            'a nonempty string: bucket should NOT be shorter than 1 characters'
    },

    // Bad triggers
    {
        config: { pre: 'hello' },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.pre should be array'
    },
    {
        config: { pre: [ 'hello' ] },
        errname: 'NotFunctionError',
        errmsg: 'trigger not function must be [Function]'
    },
    {
        config: { pre: [ '"hello"' ] },
        errname: 'NotFunctionError',
        errmsg: 'pre must be [Function]'
    },
    {
        config: { post: 'hello' },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.post should be array'
    },
    {
        config: { post: [ 'hello' ] },
        errname: 'NotFunctionError',
        errmsg: 'trigger not function must be [Function]'
    },
    {
        config: { post: [ '"hello"' ] },
        errname: 'NotFunctionError',
        errmsg: 'post must be [Function]'
    },

    // Bad values for bucket.index
    {
        config: { index: 5 },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.index should be object'
    },
    {
        config: { index: [ ] },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.index should be object'
    },

    // Bad indexes
    {
        config: { index: { foo: null } },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.index[\'foo\'] should be object'
    },
    {
        config: { index: { foo: [] } },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.index[\'foo\'] should be object'
    },
    {
        config: { index: { foo: { } } },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.index[\'foo\'] should have required property \'type\''
    },
    {
        config: {
            index: {
               foo: { type: 'invalid' }
            }
        },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.index[\'foo\'].type should be ' +
            'equal to one of the allowed values'
    },
    {
        config: {
            index: {
                foo: {
                    type: 'string',
                    unique: 'yes'
                }
            }
        },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.index[\'foo\'].unique should be boolean'
    },
    {
        config: {
            index: {
                foo: {
                    type: 'string',
                    invalid: 'foo'
                }
            }
        },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.index[\'foo\'] should NOT have additional properties'
    },

    // Bad values for bucket.options
    {
        config: { options: 'opts' },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.options should be object'
    },
    {
        config: { options: [ ] },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.options should be object'
    },

    // Bad bucket versions
    {
        config: { options: { version: '2' } },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.options.version should be integer'
    },
    {
        config: { options: { version: 2.2 } },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.options.version should be integer'
    },
    {
        config: { options: { version: -2 } },
        errname: 'InvalidBucketConfigError',
        errmsg: 'bucket.options.version should be >= 0'
    }
];

///--- Tests


BAD_BUCKETS.forEach(function (cfg) {
    cfg = jsprim.mergeObjects(DEFAULT_BUCKET, cfg);
    assert.string(cfg.errname, 'errname');
    assert.string(cfg.errmsg, 'errmsg');
    assert.string(cfg.name, 'name');
    assert.object(cfg.config, 'config');

    test(cfg.errname + ': ' + cfg.errmsg, function (t) {
        helper.makeFastRequest({
            log: LOG,
            call: {
                rpcmethod: 'createBucket',
                rpcargs: [ cfg.name, cfg.config, {} ],
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
