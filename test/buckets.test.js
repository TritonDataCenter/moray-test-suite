/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

var jsprim = require('jsprim');
var tape = require('tape');
var uuid = require('libuuid').create;
var vasync = require('vasync');
var VError = require('verror');

var helper = require('./helper.js');



///--- Globals

var FULL_CFG = {
    index: {
        str: {
            type: 'string'
        },
        str_u: {
            type: 'string',
            unique: true
        },
        num: {
            type: 'number'
        },
        num_u: {
            type: 'number',
            unique: true
        },
        bool: {
            type: 'boolean'
        },
        bool_u: {
            type: 'boolean',
            unique: true
        },
        ip: {
            type: 'ip'
        },
        ip_u: {
            type: 'ip',
            unique: true
        },
        subnet: {
            type: 'subnet'
        },
        subnet_u: {
            type: 'subnet',
            unique: true
        }
    },
    pre: [function onePre(req, cb) { cb(); }],
    post: [function onePost(req, cb) { cb(); }],
    options: {}
};

var c; // client
var server;
var b; // bucket

function test(name, setup) {
    tape.test(name + ' - setup', function (t) {
        b = 'moray_unit_test_' + uuid().substr(0, 7);
        helper.createServer(null, function (s) {
            server = s;
            c = helper.createClient();
            c.on('connect', t.end.bind(t));
        });
    });

    tape.test(name + ' - main', function (t) {
        setup(t);
    });

    tape.test(name + ' - teardown', function (t) {
        // May or may not exist, just blindly ignore
        c.delBucket(b, function () {
            c.once('close', function () {
                helper.cleanupServer(server, function () {
                    t.pass('closed');
                    t.end();
                });
            });
            c.close();
        });
    });
}


///--- Helpers

function assertBucket(t, bucket, cfg) {
    t.ok(bucket);
    if (!bucket)
        return (undefined);
    t.equal(bucket.name, b);
    t.ok(bucket.mtime instanceof Date);
    t.deepEqual(bucket.index, (cfg.index || {}));
    t.ok(Array.isArray(bucket.pre));
    t.ok(Array.isArray(bucket.post));
    t.equal(bucket.pre.length, (cfg.pre || []).length);
    t.equal(bucket.post.length, (cfg.post || []).length);

    if (bucket.pre.length !== (cfg.pre || []).length ||
        bucket.post.length !== (cfg.post || []).length)
        return (undefined);
    var i;
    for (i = 0; i < bucket.pre.length; i++)
        t.equal(bucket.pre[i].toString(), cfg.pre[i].toString());
    for (i = 0; i < bucket.post.length; i++)
        t.equal(bucket.post[i].toString(), cfg.post[i].toString());

    return (undefined);
}


///--- tests


test('create bucket stock config', function (t) {
    c.createBucket(b, {}, function (err) {
        t.ifError(err);
        c.getBucket(b, function (err2, bucket) {
            t.ifError(err2);
            assertBucket(t, bucket, {});
            c.listBuckets(function (err3, buckets) {
                t.ifError(err3);
                t.ok(buckets);
                t.ok(buckets.length);
                t.end();
            });
        });
    });
});


test('create bucket loaded', function (t) {
    c.createBucket(b, FULL_CFG, function (err) {
        t.ifError(err);
        c.getBucket(b, function (err2, bucket) {
            t.ifError(err2);
            assertBucket(t, bucket, FULL_CFG);
            t.end();
        });
    });
});


test('update bucket', function (t) {
    c.createBucket(b, FULL_CFG, function (err) {
        t.ifError(err);
        var cfg = jsprim.deepCopy(FULL_CFG);
        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        cfg.post.push(function two(req, cb) {
            cb();
        });
        c.updateBucket(b, cfg, function (err2) {
            t.ifError(err2);
            c.getBucket(b, function (err3, bucket) {
                t.ifError(err3);
                assertBucket(t, bucket, cfg);
                t.end();
            });
        });
    });
});


test('update bucket (versioned ok 0->1)', function (t) {
    c.createBucket(b, FULL_CFG, function (err) {
        t.ifError(err);
        var cfg = jsprim.deepCopy(FULL_CFG);
        cfg.options.version = 1;
        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        cfg.post.push(function two(req, cb) {
            cb();
        });
        c.updateBucket(b, cfg, function (err2) {
            t.ifError(err2);
            c.getBucket(b, function (err3, bucket) {
                t.ifError(err3);
                assertBucket(t, bucket, cfg);
                t.end();
            });
        });
    });
});


test('update bucket (versioned ok 1->2)', function (t) {
    var cfg = jsprim.deepCopy(FULL_CFG);

    cfg.options.version = 1;
    c.createBucket(b, FULL_CFG, function (err) {
        t.ifError(err);
        cfg = jsprim.deepCopy(FULL_CFG);
        cfg.options.version = 2;
        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        cfg.post.push(function two(req, cb) {
            cb();
        });
        c.updateBucket(b, cfg, function (err2) {
            t.ifError(err2);
            c.getBucket(b, function (err3, bucket) {
                t.ifError(err3);
                assertBucket(t, bucket, cfg);
                t.end();
            });
        });
    });
});


test('update bucket (reindex tracked)', function (t) {
    var cfg = jsprim.deepCopy(FULL_CFG);

    cfg.options.version = 1;
    c.createBucket(b, FULL_CFG, function (err) {
        t.ifError(err);
        cfg = jsprim.deepCopy(FULL_CFG);
        cfg.options.version = 2;
        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        c.updateBucket(b, cfg, function (err2) {
            t.ifError(err2);
            c.getBucket(b, function (err3, bucket) {
                t.ifError(err3);
                assertBucket(t, bucket, cfg);
                t.ok(bucket.reindex_active);
                t.ok(bucket.reindex_active['2']);
                t.end();
            });
        });
    });
});


test('update bucket (reindex disabled)', function (t) {
    var cfg = jsprim.deepCopy(FULL_CFG);

    cfg.options.version = 1;
    c.createBucket(b, FULL_CFG, function (err) {
        t.ifError(err);
        cfg = jsprim.deepCopy(FULL_CFG);
        cfg.options.version = 2;
        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        var opts = {
            no_reindex: true
        };
        c.updateBucket(b, cfg, opts, function (err2) {
            t.ifError(err2);
            c.getBucket(b, function (err3, bucket) {
                t.ifError(err3);
                assertBucket(t, bucket, cfg);
                t.notOk(bucket.reindex_active);
                t.end();
            });
        });
    });
});


test('update bucket (null version, reindex disabled)', function (t) {
    var cfg = jsprim.deepCopy(FULL_CFG);

    cfg.options.version = 0;
    c.createBucket(b, FULL_CFG, function (err) {
        t.ifError(err);
        cfg = jsprim.deepCopy(FULL_CFG);
        cfg.options.version = 0;
        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        c.updateBucket(b, cfg, function (err2) {
            t.ifError(err2);
            c.getBucket(b, function (err3, bucket) {
                t.ifError(err3);
                assertBucket(t, bucket, cfg);
                t.notOk(bucket.reindex_active);
                t.end();
            });
        });
    });
});


test('update bucket (versioned not ok 1 -> 0)', function (t) {
    var cfg = jsprim.deepCopy(FULL_CFG);
    cfg.options.version = 1;

    c.createBucket(b, cfg, function (err) {
        t.ifError(err);

        cfg = jsprim.deepCopy(FULL_CFG);
        cfg.options.version = 0;

        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        cfg.post.push(function two(req, cb) {
            cb();
        });

        c.updateBucket(b, cfg, function (err2) {
            t.ok(err2);
            if (err2) {
                t.ok(VError.findCauseByName(
                    err2, 'BucketVersionError') !== null);
                t.ok(err2.message);
            }
            t.end();
        });
    });
});


test('update bucket (versioned not ok 2 -> 1)', function (t) {
    var cfg = jsprim.deepCopy(FULL_CFG);
    cfg.options.version = 2;

    c.createBucket(b, cfg, function (err) {
        t.ifError(err);

        cfg = jsprim.deepCopy(FULL_CFG);
        cfg.options.version = 1;

        cfg.index.foo = {
            type: 'string',
            unique: false
        };
        cfg.post.push(function two(req, cb) {
            cb();
        });

        c.updateBucket(b, cfg, function (err2) {
            t.ok(err2);
            if (err2) {
                t.ok(VError.findCauseByName(
                    err2, 'BucketVersionError') !== null);
                t.ok(err2.message);
            }
            t.end();
        });
    });
});


test('create bucket bad index type', function (t) {
    c.createBucket(b, {index: {foo: 'foo'}}, function (err) {
        t.ok(err);
        t.ok(VError.findCauseByName(err, 'InvalidBucketConfigError') !== null);
        t.ok(err.message);
        t.end();
    });
});


test('create bucket triggers not function', function (t) {
    c.createBucket(b, {pre: ['foo']}, function (err) {
        t.ok(err);
        t.ok(VError.findCauseByName(err, 'NotFunctionError') !== null);
        t.ok(err.message);
        t.end();
    });
});


test('get bucket 404', function (t) {
    c.getBucket(uuid().substr(0, 7), function (err) {
        t.ok(err);
        t.ok(VError.findCauseByName(err, 'BucketNotFoundError') !== null);
        t.ok(err.message);
        t.end();
    });
});


test('delete missing bucket', function (t) {
    c.delBucket(uuid().substr(0, 7), function (err) {
        t.ok(err);
        t.ok(VError.findCauseByName(err, 'BucketNotFoundError') !== null);
        t.ok(err.message);
        t.end();
    });
});


test('MORAY-378 - Bucket cache cleared on bucket delete', function (t) {
    vasync.pipeline({ funcs: [
        function (_, cb) { c.createBucket(b, {}, cb); },
        function (_, cb) {
            c.updateBucket(b, {
                index: { field: { type: 'number' } },
                options: { version: 2 }
            }, cb);
        },
        function (_, cb) { c.putObject(b, 'key', { field: 5 }, cb); },
        function (_, cb) { c.delBucket(b, cb); },
        function (_, cb) { c.createBucket(b, {}, cb); },
        function (_, cb) { c.putObject(b, 'key', { field: 5 }, cb); }
    ]}, function (err) {
        t.error(err, 'Finish without error');
        t.end();
    });
});


test('MORAY-378 - Bucket cache cleared on bucket update', function (t) {
    var schema = {
        index: { field: { type: 'string' } },
        options: { version: 2 }
    };

    vasync.pipeline({ funcs: [
        function (_, cb) { c.createBucket(b, {}, cb); },
        function (_, cb) { c.putObject(b, 'key1', {}, cb); },
        function (_, cb) { c.updateBucket(b, schema, cb); },
        function (_, cb) { c.putObject(b, 'key2', { field: 'foo' }, cb); },
        function (_, cb) {
            var count = 0;
            var res =
                c.sql('SELECT * FROM ' + b + ' WHERE _key = $1;', ['key2']);
            res.on('record', function (r) {
                t.equal(r._key, 'key2', 'correct object returned for key');
                t.equal(r.field, 'foo', '"field" column had value inserted');
                count += 1;
            });
            res.on('error', cb);
            res.on('end', function () {
                t.equal(count, 1, 'one row returned');
                cb();
            });
        },
    ]}, function (err) {
        t.error(err, 'Finish without error');
        t.end();
    });
});
