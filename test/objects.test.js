/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var jsprim = require('jsprim');
var tape = require('tape');
var once = require('once');
var libuuid = require('libuuid');
var vasync = require('vasync');
var util = require('util');
var net = require('net');
var VError = require('verror');

var fmt = util.format;
var helper = require('./helper.js');



///--- Globals

var uuid = {
    v1: libuuid.create,
    v4: libuuid.create
};

var BUCKET_CFG = {
    index: {
        date: {
            type: 'date'
        },
        date_a: {
            type: '[date]'
        },
        date_u: {
            type: 'date',
            unique: true
        },
        dater: {
            type: 'daterange'
        },
        dater_u: {
            type: 'daterange',
            unique: true
        },
        str: {
            type: 'string'
        },
        str_a: {
            type: '[string]'
        },
        str_u: {
            type: 'string',
            unique: true
        },
        str_2: {
            type: 'string'
        },
        num: {
            type: 'number'
        },
        num_a: {
            type: '[number]'
        },
        num_u: {
            type: 'number',
            unique: true
        },
        numr: {
            type: 'numrange'
        },
        numr_u: {
            type: 'numrange',
            unique: true
        },
        bool: {
            type: 'boolean'
        },
        bool_a: {
            type: '[boolean]'
        },
        bool_u: {
            type: 'boolean',
            unique: true
        },
        ip: {
            type: 'ip'
        },
        ip_a: {
            type: '[ip]'
        },
        ip_u: {
            type: 'ip',
            unique: true
        },
        mac: {
            type: 'mac'
        },
        mac_a: {
            type: '[mac]'
        },
        mac_u: {
            type: 'mac',
            unique: true
        },
        subnet: {
            type: 'subnet'
        },
        subnet_a: {
            type: '[subnet]'
        },
        subnet_u: {
            type: 'subnet',
            unique: true
        },
        uuid: {
            type: 'uuid'
        },
        uuid_u: {
            type: 'uuid',
            unique: true
        }
    },
    pre: [function (req, cb) {
        var v = req.value;
        if (v.pre)
            v.pre = 'pre_overwrite';

        cb();
    }],
    post: [function (req, cb) {
        cb();
    }],
    options: {
        version: 1,
        guaranteeOrder: true
    }
};

var c; // client
var server;
var b; // bucket

var INDEXES = Object.keys(BUCKET_CFG.index);

var NUM_NON_UNIQUE_INDEXES = INDEXES.reduce(function (acc, i) {
    return acc + (BUCKET_CFG.index[i].unique ? 0 : 1);
}, 0);

var NUM_UNIQUE_INDEXES = INDEXES.reduce(function (acc, i) {
    return acc + (BUCKET_CFG.index[i].unique ? 1 : 0);
}, 0);

var NUM_ARRAY_INDEXES = INDEXES.reduce(function (acc, i) {
    return acc + (BUCKET_CFG.index[i].type[0] === '[' ? 1 : 0);
}, 0);

function test(name, setup) {
    tape.test(name + ' - setup', function (t) {
        b = 'moray_unit_test_' + uuid.v4().substr(0, 7);
        helper.createServer(null, function (s) {
            server = s;
            c = helper.createClient();
            c.on('connect', function () {
                c.createBucket(b, BUCKET_CFG, function (err) {
                    t.ifError(err);
                    t.end();
                });
            });
        });
    });

    tape.test(name + ' - main', function (t) {
        setup(t);
    });

    tape.test(name + ' - teardown', function (t) {
        c.delBucket(b, function (err) {
            t.ifError(err);
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

function assertObject(t, obj, k, v) {
    t.ok(obj);
    if (!obj)
        return (undefined);

    t.equal(obj.bucket, b, 'has correct "bucket"');
    t.equal(obj.key, k, 'has correct "key"');
    t.deepEqual(obj.value, v, 'matches expected value');
    t.ok(obj._id, 'has "_id"');
    t.ok(obj._etag, 'has "_etag"');
    t.ok(obj._mtime, 'has "_mtime"');
    if (v.vnode) {
        t.ok(obj.value.vnode, 'has "vnode"');
    }
    return (undefined);
}

///--- Tests

test('get object 404', function (t) {
    c.getObject(b, uuid.v4().substr(0, 7), function (err) {
        t.ok(err);
        t.ok(VError.findCauseByName(err, 'ObjectNotFoundError') !== null);
        t.ok(err.message);
        t.end();
    });
});


test('del object 404', function (t) {
    c.delObject(b, uuid.v4().substr(0, 7), function (err) {
        t.ok(err);
        t.ok(VError.findCauseByName(err, 'ObjectNotFoundError') !== null);
        t.ok(err.message);
        t.end();
    });
});


test('CRUD object', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi',
        vnode: 2
    };
    var v2 = {
        str: 'hello world',
        pre: 'hi'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, function (err, meta) {
                if (err)
                    return (cb(err));

                t.ok(meta);
                if (meta)
                    t.ok(meta.etag);
                return (cb());
            });
        }, function get(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                assertObject(t, obj, k, v);
                return (cb());
            });
        }, function overwrite(_, cb) {
            c.putObject(b, k, v2, cb);
        }, function getAgain(_, cb) {
            c.getObject(b, k, {noCache: true}, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                v2.pre = 'pre_overwrite';
                assertObject(t, obj, k, v2);
                return (cb());
            });
        }, function del(_, cb) {
            c.delObject(b, k, cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('get object (cached)', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, function (err, meta) {
                if (err)
                    return (cb(err));

                t.ok(meta);
                if (meta)
                    t.ok(meta.etag);
                return (cb());
            });
        }, function get(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                assertObject(t, obj, k, v);
                return (cb());
            });
        }, function getAgain(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                assertObject(t, obj, k, v);
                return (cb());
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('CRUD objects unique indexes', function (t) {
    var k = uuid.v4();
    var k2 = uuid.v4();
    var v = {
        str_u: 'hi'
    };
    var v2 = {
        str_u: 'hi'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function putFail(_, cb) {
            c.putObject(b, k2, v2, function (err) {
                t.ok(err);
                t.ok(VError.findCauseByName(err,
                    'UniqueAttributeError') !== null);
                cb();
            });
        }, function delK1(_, cb) {
            c.delObject(b, k, cb);
        }, function putK2(_, cb) {
            c.putObject(b, k2, v2, cb);
        }, function delK2(_, cb) {
            c.delObject(b, k2, cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('put object w/etag ok', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };
    var v2 = {
        str: 'hello world'
    };
    var etag;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function get(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                assertObject(t, obj, k, v);
                etag = obj._etag;
                return (cb());
            });
        }, function overwrite(_, cb) {
            c.putObject(b, k, v2, {etag: etag}, cb);
        }, function getAgain(_, cb) {
            c.getObject(b, k, {noCache: true}, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                assertObject(t, obj, k, v2);
                return (cb());
            });
        }, function del(_, cb) {
            c.delObject(b, k, cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('del object w/etag ok', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };
    var etag;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function get(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                assertObject(t, obj, k, v);
                etag = obj._etag;
                return (cb());
            });
        }, function del(_, cb) {
            c.delObject(b, k, {etag: etag}, cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('put object w/etag conflict', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function overwrite(_, cb) {
            c.putObject(b, k, {}, {etag: 'foo'}, function (err) {
                t.ok(err);
                if (err) {
                    t.ok(VError.findCauseByName(err,
                        'EtagConflictError') !== null);
                }
                cb();
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);

        t.end();
    });
});


test('del object w/etag conflict', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function drop(_, cb) {
            c.delObject(b, k, {etag: 'foo'}, function (err) {
                t.ok(err);
                if (err) {
                    err = VError.findCauseByName(err, 'EtagConflictError');
                    t.ok(err !== null);
                    t.ok(err.context);
                    if (err.context) {
                        var ctx = err.context;
                        t.equal(ctx.bucket, b);
                        t.equal(ctx.key, k);
                        t.equal(ctx.expected, 'foo');
                        t.ok(ctx.actual);
                    }
                }
                cb();
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);

        t.end();
    });
});

test('MORAY-406: Put an object with null values', function (t) {
    var k = uuid.v4();
    var v = {
        date: null,
        date_a: null,
        date_u: null,
        dater: null,
        dater_u: null,
        str: null,
        str_a: null,
        str_u: null,
        str_2: null,
        num: null,
        num_a: null,
        num_u: null,
        numr: null,
        numr_u: null,
        bool: null,
        bool_a: null,
        bool_u: null,
        ip: null,
        ip_a: null,
        ip_u: null,
        mac: null,
        mac_a: null,
        mac_u: null,
        subnet: null,
        subnet_a: null,
        subnet_u: null,
        uuid: null,
        uuid_u: null
    };

    t.equal(Object.keys(v).length, INDEXES.length, 'all fields tested');

    t.test('putObject()', function (t2) {
        c.putObject(b, k, v, function (err) {
            t2.ifError(err);
            t2.end();
        });
    });

    t.test('getObject()', function (t2) {
        c.getObject(b, k, function (err, obj) {
            t2.ifError(err);
            assertObject(t2, obj, k, v);
            t2.end();
        });
    });
});


test('MORAY-406: Reject updateObjects() w/ null values for now', function (t) {
    var fields = Object.keys(BUCKET_CFG.index);

    t.plan(fields.length);

    fields.forEach(function (field) {
        t.test('update ' + field + '=null', function (t2) {
            var obj = {};
            obj[field] = null;

            c.updateObjects(b, obj, '(str_2=hello)', function (err, res) {
                t2.ok(err, 'updateObjects() should fail');
                t2.notOk(res, 'No result object');
                if (err) {
                    t2.ok(VError.hasCauseWithName(err, 'NotNullableError'),
                        'Failed due to missing index');
                }
                t2.end();
            });
        });
    });
});


/*
 * This test looks just like the previous one, but uses a client that unwraps
 * Fast errors.  This option is provided for compatibility, and this test exists
 * to verify that functionality.
 *
 * This does the right thing even for Moray clients that don't support
 * "unwrapErrors", because those already behave this way.
 */
test('legacy error unwrapping behavior', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };
    var c2;

    vasync.pipeline({
        funcs: [ function mkClient(_, cb) {
            c2 = helper.createClient({ 'unwrapErrors': true });
            c2.on('connect', function () { cb(); });
        }, function put(_, cb) {
            c2.putObject(b, k, v, cb);
        }, function drop(_, cb) {
            c2.delObject(b, k, {etag: 'foo'}, function (err) {
                t.ok(err);
                if (err) {
                    t.equal(err.name, 'EtagConflictError');
                    t.ok(err.context);
                    if (err.context) {
                        var ctx = err.context;
                        t.equal(ctx.bucket, b);
                        t.equal(ctx.key, k);
                        t.equal(ctx.expected, 'foo');
                        t.ok(ctx.actual);
                    }
                }
                cb();
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        c2.once('close', function () { t.end(); });
        c2.close();
    });
});


test('MANTA-980 - null etag support', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };
    var v2 = {
        str: 'hello world'
    };
    var etag;
    var value;

    function get_cb(cb) {
        function _cb(err, obj) {
            if (err) {
                cb(err);
                return;
            }

            t.ok(obj);
            if (obj) {
                assertObject(t, obj, k, value);
                etag = obj._etag;
            }
            cb();
        }
        return (_cb);
    }

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            value = v;
            c.putObject(b, k, value, {etag: null}, cb);
        }, function get(_, cb) {
            c.getObject(b, k, get_cb(cb));
        }, function overwrite(_, cb) {
            value = v2;
            c.putObject(b, k, value, {etag: etag}, cb);
        }, function getAgain(_, cb) {
            c.getObject(b, k, {noCache: true}, get_cb(cb));
        }, function putFail(_, cb) {
            c.putObject(b, k, v, {etag: null}, function (err) {
                t.ok(err);
                if (err) {
                    err = VError.findCauseByName(err, 'EtagConflictError');
                    t.ok(err !== null);
                    t.ok(err.context);
                    t.equal(err.context.bucket, b);
                    t.equal(err.context.key, k);
                    t.equal(err.context.expected, 'null');
                    t.equal(err.context.actual, etag);
                }
                cb();
            });
        }, function del(_, cb) {
            c.delObject(b, k, {etag: etag}, cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('find (like marlin)', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello',
        str_2: 'world'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(&(str=hello)(!(str_2=usa)))';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                t.ok(obj);
                if (!obj)
                    return (undefined);

                t.equal(obj.bucket, b);
                t.equal(obj.key, k);
                t.deepEqual(obj.value, v);
                t.ok(obj._id);
                t.ok(obj._count);
                t.equal(typeof (obj._count), 'number');
                t.ok(obj._etag);
                t.ok(obj._mtime);
                found = true;
                return (undefined);
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('find _mtime', function (t) {
    var k = uuid.v4();
    var now = Date.now();
    var v = {
        str: 'hello',
        str_2: 'world'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            /* this is sensitive to clock skew between hosts */
            setTimeout(cb, 1000);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(_mtime>=' + now + ')';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                t.ok(obj, 'record is truthy');
                if (!obj)
                    return (undefined);

                t.equal(obj.bucket, b);
                t.equal(obj.key, k);
                t.deepEqual(obj.value, v);
                t.ok(obj._id, '_id property is truthy');
                t.ok(obj._etag, '_etag property is truthy');
                t.ok(obj._mtime, '_mtime is truthy');
                found = true;
                return (undefined);
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found, 'found a record');
        t.end();
    });
});


test('find _key', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello',
        str_2: 'world'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            setTimeout(cb, 500);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(_key=' + k + ')';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                t.ok(obj);
                if (!obj)
                    return (undefined);

                t.equal(obj.bucket, b);
                t.equal(obj.key, k);
                t.deepEqual(obj.value, v);
                t.ok(obj._id);
                t.ok(obj._etag);
                t.ok(obj._mtime);
                found = true;
                return (undefined);
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('find MANTA-156', function (t) {
    var k = uuid.v4();
    var v = {
        num: 0,
        num_u: 1
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            setTimeout(cb, 500);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(num>=0)';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                t.ok(obj);
                if (!obj)
                    return (undefined);

                t.equal(obj.bucket, b);
                t.equal(obj.key, k);
                t.deepEqual(obj.value, v);
                t.ok(obj._id);
                t.ok(obj._etag);
                t.ok(obj._mtime);
                found = true;
                return (undefined);
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('non-indexed AND searches (MANTA-317)', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello',
        cow: 'moo'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            setTimeout(cb, 500);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(&(str=hello)(!(cow=woof)))';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                t.ok(obj);
                if (!obj)
                    return (undefined);

                t.equal(obj.bucket, b);
                t.equal(obj.key, k);
                t.deepEqual(obj.value, v);
                t.ok(obj._id);
                t.ok(obj._etag);
                t.ok(obj._mtime);
                found = true;
                return (undefined);
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('_txn_snap on update', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hi'
    };
    var txn;

    vasync.pipeline({
        funcs: [ function create(_, cb) {
            c.putObject(b, k, v, cb);
        }, function getOne(_, cb) {
            c.getObject(b, k, {noCache: true}, function (err, obj) {
                if (err) {
                    cb(err);
                } else {
                    t.ok(obj);
                    assertObject(t, obj, k, v);
                    t.ok(obj._txn_snap);
                    txn = obj._txn_snap;
                    cb();
                }
            });
        }, function update(_, cb) {
            c.putObject(b, k, v, cb);
        }, function getTwo(_, cb) {
            c.getObject(b, k, {noCache: true}, function (err, obj) {
                if (err) {
                    cb(err);
                } else {
                    t.ok(obj);
                    assertObject(t, obj, k, v);
                    t.ok(obj._txn_snap);
                    t.notEqual(txn, obj._txn_snap);
                    t.ok(obj._txn_snap > txn);
                    cb();
                }
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);

        t.end();
    });
});


test('find _txn_snap', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello',
        str_2: 'world'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            setTimeout(cb, 500);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(&(_txn_snap>=1)(_id>=1))';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                t.ok(obj);
                if (!obj)
                    return (undefined);

                t.equal(obj.bucket, b);
                t.equal(obj.key, k);
                t.deepEqual(obj.value, v);
                t.ok(obj._id);
                t.ok(obj._etag);
                t.ok(obj._mtime);
                t.ok(obj._txn_snap);
                found = true;
                return (undefined);
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('trackModification is no longer supported', function (t) {
    var bname = 'moray_unit_test_track_mod_' + uuid.v4().substr(0, 7);
    var bcfg = {
        options: {
            version: 9000,
            trackModification: true
        }
    };

    c.createBucket(bname, bcfg, function (err) {
        if (!err) {
            t.fail('bucket creation with "trackModification" must fail');
            t.end();
            return;
        }

        t.ok(err.message.match(/no longer supported/),
          'bucket creation with "trackModification" reports failure');
        t.end();
    });
});


test('batch put objects', function (t) {
    var requests = [
        {
            bucket: b,
            key: uuid.v4(),
            value: {
                foo: 'bar'
            }
        },
        {
            bucket: b,
            key: uuid.v4(),
            value: {
                bar: 'baz'
            }
        }
    ];

    c.batch(requests, function (err, meta) {
        t.ifError(err);
        t.ok(meta);
        if (meta) {
            t.ok(meta.etags);
            if (meta.etags) {
                t.ok(Array.isArray(meta.etags));
                t.equal(meta.etags.length, 2);
                meta.etags.forEach(function (e) {
                    t.equal(b, e.bucket);
                    t.ok(e.key);
                    t.ok(e.etag);
                });
            }
        }
        c.getObject(b, requests[0].key, function (er2, obj) {
            t.ifError(er2);
            t.ok(obj);
            if (obj)
                t.deepEqual(obj.value, requests[0].value);

            var r = requests[1];
            c.getObject(b, r.key, function (err3, obj2) {
                t.ifError(err3);
                t.ok(obj2);
                if (obj2)
                    t.deepEqual(obj2.value, r.value);
                t.end();
            });
        });
    });
});


test('batch put with bad _value', function (t) {
    // In a future node-moray, this shouldn't even be possible, but for now it
    // needs to be dealt with.
    var k = uuid.v4();
    var requests = [
        {
            bucket: b,
            key: k,
            value: {
                foo: 'bar'
            },
            options: {
                _value: '{"this":"is", "bs":[}'
            }
        }
    ];

    vasync.pipeline({
        funcs: [
            function prepBucket(_, cb) {
                var cfg = jsprim.deepCopy(BUCKET_CFG);
                // Simplify test by removing pre/post bucket actions
                // (Required for positive verification)
                delete cfg.pre;
                delete cfg.post;
                cfg.options.version = 2;
                c.updateBucket(b, cfg, cb);
            },
            function put(_, cb) {
                c.batch(requests, cb);
            },
            function checkValid(_, cb) {
                c.getObject(b, k, cb);
            },
            function cleanup(_, cb) {
                c.delObject(b, k, cb);
            }
        ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('batch delete object', function (t) {
    var k = uuid.v4();
    var v = { str: 'hi' };
    var requests = [
        {
            operation: 'delete',
            bucket: b,
            key: k
        }
    ];

    vasync.pipeline({
        funcs: [
            function put(_, cb) {
                c.putObject(b, k, v, cb);
            },
            function checkPresent(_, cb) {
                c.getObject(b, k, cb);
            },
            function batchDel(_, cb) {
                c.batch(requests, cb);
            },
            function checkGone(_, cb) {
                c.getObject(b, k, function (err) {
                    t.ok(err);
                    t.ok(VError.findCauseByName(
                        err, 'ObjectNotFoundError') !== null);
                    t.ok(err.message);
                    cb();
                });
            }
        ],
        arg: {}
    }, function (err) {
        t.ifError(err);

        t.end();
    });
});


test('update objects no keys', function (t) {
    var requests = [];
    for (var i = 0; i < 10; i++) {
        requests.push({
            bucket: b,
            key: uuid.v4().substr(0, 7),
            value: {
                num: 20,
                num_u: i,
                str: 'foo',
                str_u: uuid.v4().substr(0, 7)
            }
        });
    }

    c.batch(requests, function (put_err) {
        t.ifError(put_err);
        if (put_err) {
            t.end();
            return;
        }

        c.updateObjects(b, {}, '(num>=20)', function (err) {
            t.ok(err);
            t.ok(VError.findCauseByName(err, 'FieldUpdateError') !== null);
            t.end();
        });
    });
});


test('update objects ok', function (t) {
    var requests = [];
    for (var i = 0; i < 10; i++) {
        requests.push({
            bucket: b,
            key: uuid.v4().substr(0, 7),
            value: {
                num: 20,
                num_u: i,
                str: 'foo',
                str_u: uuid.v4().substr(0, 7)
            }
        });
    }

    c.batch(requests, function (put_err) {
        t.ifError(put_err);
        if (put_err) {
            t.end();
            return;
        }

        var fields = {str: 'bar'};
        c.updateObjects(b, fields, '(num>=20)', function (err, meta) {
            t.ifError(err);
            t.ok(meta);
            if (!meta) {
                t.end();
                return;
            }
            t.ok(meta.etag);

            c.getObject(b, requests[0].key, function (err2, obj) {
                t.ifError(err2);
                t.ok(obj);
                if (obj) {
                    t.equal(obj.value.str, 'bar');
                    t.equal(obj._etag, meta.etag);
                }

                t.end();
            });
        });
    });
});


test('update objects w/array (ufds - no effect)', function (t) {
    var requests = [];
    for (var i = 0; i < 10; i++) {
        requests.push({
            bucket: b,
            key: uuid.v4().substr(0, 7),
            value: {
                str: ['foo']
            }
        });
    }

    c.batch(requests, function (put_err) {
        t.ifError(put_err);
        if (put_err) {
            t.end();
            return;
        }

        var fields = {str: 'bar'};
        c.updateObjects(b, fields, '(str=foo)', function (err, meta) {
            t.ifError(err);
            t.ok(meta);
            if (!meta) {
                t.end();
                return;
            }
            t.ok(meta.etag);

            var k = requests[0].key;
            var o = {noCache: true};
            c.getObject(b, k, o, function (err2, obj) {
                t.ifError(err2);
                t.ok(obj);
                if (obj) {
                    t.ok(Array.isArray(obj.value.str));
                    t.notOk(obj.value.str_u);
                    t.equal(obj.value.str[0], 'foo');
                    t.equal(obj._etag, meta.etag);
                }

                t.end();
            });
        });
    });
});


test('batch put/update', function (t) {
    var requests = [];
    for (var i = 0; i < 10; i++) {
        requests.push({
            bucket: b,
            key: uuid.v4().substr(0, 7),
            value: {
                num: 20,
                num_u: i,
                str: 'foo',
                str_u: uuid.v4().substr(0, 7)
            }
        });
    }

    c.batch(requests, function (init_err) {
        t.ifError(init_err);

        var ops = [
            {
                bucket: b,
                key: requests[0].key,
                value: {
                    num: 10,
                    str: 'baz'
                }
            },
            {
                bucket: b,
                operation: 'update',
                fields: {
                    str: 'bar'
                },
                filter: '(num_u>=5)'
            }
        ];
        c.batch(ops, function (err, meta) {
            t.ifError(err);
            t.ok(meta);
            t.ok(meta.etags);
            var req = c.findObjects(b, '(num_u>=0)');
            req.once('error', function (e) {
                t.ifError(e);
                t.end();
            });
            req.once('end', function () {
                t.end();
            });
            req.on('record', function (r) {
                t.equal(r.bucket, b);
                t.ok(r.key);
                var v = r.value;
                if (v.num_u >= 5) {
                    t.equal(v.str, 'bar');
                } else if (r.key === requests[0].key) {
                    t.equal(v.str, 'baz');
                } else {
                    t.equal(v.str, 'foo');
                }
            });
        });
    });
});


test('delete many objects ok', function (t) {
    var requests = [];
    for (var i = 0; i < 10; i++) {
        requests.push({
            bucket: b,
            key: uuid.v4().substr(0, 7),
            value: {
                num: 20,
                num_u: i,
                str: 'foo',
                str_u: uuid.v4().substr(0, 7)
            }
        });
    }

    c.batch(requests, function (put_err) {
        t.ifError(put_err);
        if (put_err) {
            t.end();
            return;
        }

        c.deleteMany(b, '(num>=20)', function (err) {
            t.ifError(err);
            t.end();
        });
    });
});

test('get tokens unsupported', function (t) {
    c.getTokens(function (err, res) {
        t.notOk(res);
        t.ok(err);
        t.end();
    });
});


test('MORAY-147 (sqli)', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello',
        str_2: 'world'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(&(str=hel\')(!(str_2=usa)))';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                found = true;
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(!found);
        t.end();
    });
});



test('MORAY-148 (foo=bar=*)', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello=world',
        str_2: 'world=hello'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(|(str=hello=*)(str_2=world=*))';
            var req = c.findObjects(b, f);
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                found = true;
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('MORAY-166: deleteMany with LIMIT', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello=world'
    };
    var N = 35;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            cb = once(cb);

            var done = 0;
            function _cb(err) {
                if (err) {
                    cb(err);
                } else if (++done === N) {
                    cb();
                }
            }

            for (var i = 0; i < N; i++)
                c.putObject(b, k + '' + i, v, _cb);

        }, function delMany(_, cb) {
            cb = once(cb);

            var _opts = {
                limit: Math.floor(N / 4)
            };

            (function drop() {
                function _cb(err, meta) {
                    if (err) {
                        cb(err);
                        return;
                    }

                    t.ok(meta);
                    if (!meta) {
                        cb(new Error('boom'));
                        return;
                    }
                    t.ok(meta.count <= _opts.limit);
                    if (meta.count > 0) {
                        drop();
                    } else {
                        cb();
                    }
                }

                c.deleteMany(b, '(str=*)', _opts, _cb);
            })();
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});



test('MORAY-166: update with LIMIT', function (t) {
    var k = uuid.v4();
    var v = {
        str: 'hello=world'
    };
    var N = 35;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            cb = once(cb);

            var done = 0;
            function _cb(err) {
                if (err) {
                    cb(err);
                } else if (++done === N) {
                    cb();
                }
            }

            for (var i = 0; i < N; i++)
                c.putObject(b, k + '' + i, v, _cb);

        }, function updateMany(_, cb) {
            cb = once(cb);

            var _opts = {
                limit: Math.floor(N / 4)
            };

            function _cb(err, meta) {
                if (err) {
                    cb(err);
                    return;
                }

                t.ok(meta);
                if (!meta) {
                    cb(new Error('boom'));
                    return;
                }

                t.equal(meta.count, _opts.limit);
                cb();
            }

            c.updateObjects(b, {str: 'fo'}, '(str=*)', _opts, _cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('MORAY-403: updateObjects() of every type', function (t) {
    // UUIDs of objects that will go unaffected by updateObjects()
    var ignored = [ uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4() ];
    var ignobj = { str_2: 'hello' };

    // Keys of affected objects
    var k1 = uuid.v4();
    var k2 = uuid.v4();
    var k3 = uuid.v4();

    // Values by which we find objects
    var v1 = uuid.v4();
    var v2 = uuid.v4();
    var v3 = uuid.v4();
    var v4 = uuid.v4();

    var changed1 = {
        str_2: v2,

        date: '2018-06-14T20:36:55.151Z',
        date_u: '1920-04-13T12:30:00.000Z',
        date_a: [ '2015-06-29T01:23:45.678Z', '2016-06-15T00:00:00.000Z' ],
        dater: '[1861-04-12T00:00:00.000Z,1865-05-09T00:00:00.000Z]',
        dater_u: '(1835-01-25T04:00:00.000Z,1835-01-25T07:00:00.000Z)',
        str: 'a1',
        str_a: [ 'foo', 'bar', 'baz' ],
        str_u: 'u1',
        num: 1,
        num_a: [ 1, 1, 2, 3, 5, 8 ],
        num_u: 2003,
        numr: '(9000,)',
        numr_u: '(2,100]',
        bool: true,
        bool_a: [ true, false, false, true ],
        bool_u: false,
        ip: '1.2.3.4',
        ip_a: [ '1.2.3.4', 'fd00::30e' ],
        ip_u: '127.0.0.1',
        mac: '23:56:dc:b1:6a:d6',
        mac_a: [ '1b:a5:bc:32:ab:d5', '50:f4:66:89:48:90' ],
        mac_u: '1e:da:0b:d2:93:da',
        subnet: 'fc00::/7',
        subnet_a: [ 'fe80::/10', 'fc00::/7', '10.0.0.0/8' ],
        subnet_u: '172.16.0.0/12',
        uuid: '392d019e-7723-4db7-a830-ff1a2415a5bc',
        uuid_u: '62f13c43-59aa-c188-f40b-aeca10628785'
    };

    var changed2 = {
        str_2: v3,

        date: '1823-12-02T00:00:00.000Z',
        date_u: '0528-06-21T12:03:00.000Z',
        date_a: [ '1582-10-04T23:59:59.999Z', '1582-10-15T00:00:00.000Z' ],
        dater: '[1618-05-23T00:00:00.000Z,1648-05-15T00:00:00.000Z]',
        dater_u: '(1483-11-10T00:00:00.000Z,1546-02-18T00:00:00.000Z]',
        str: 'a2',
        str_a: [ 'red', 'blue', 'green' ],
        str_u: 'u2',
        num: 2,
        num_a: [ 1, 10, 100, 1000, 10000 ],
        num_u: 3002,
        numr: '(,9000)',
        numr_u: '[2,100)',
        bool: false,
        bool_a: [ false, true, true, false ],
        bool_u: true,
        ip: '4.3.2.1',
        ip_a: [ '192.168.1.1', '2001:4860:4860::8888' ],
        ip_u: '127.0.0.2',
        mac: '3f:30:6e:af:f5:c1',
        mac_a: [
            '54:d1:2a:38:84:c9',
            '12:b9:58:25:41:5c',
            '51:8f:55:00:ac:60'
        ],
        mac_u: '01:ae:57:fa:f4:8c',
        subnet: 'fd00:1234::/64',
        subnet_a: [ 'fd4e::/64', 'fc12:3456::/64', '192.168.0.0/16' ],
        subnet_u: '172.16.1.0/24',
        uuid: '1d96d942-9be7-68ea-f9f9-e9fbe0b7a18b',
        uuid_u: '69bf5a59-869c-6da4-f2b5-dd1ed6aa977e'
    };

    var changed3 = {
        date_a: [],
        str_a: [],
        num_a: [],
        bool_a: [],
        ip_a: [],
        mac_a: [],
        subnet_a: []
    };

    t.equal(Object.keys(changed1).length, INDEXES.length, 'all fields tested');
    t.equal(Object.keys(changed2).length, INDEXES.length, 'all fields tested');
    t.equal(Object.keys(changed3).length, NUM_ARRAY_INDEXES,
        'all array fields tested');

    function doUpdate(fields, filter, count, cb) {
        c.updateObjects(b, fields, filter, function (err, res) {
            t.ifError(err, 'updateObjects() error');
            t.ok(res, 'Result object returned');
            if (res) {
                t.ok(res.etag, 'Result has "etag"');
                t.equal(res.count, count, 'Result has correct "count"');
            }
            cb(err);
        });
    }

    function checkObject(key, exp, cb) {
        c.getObject(b, key, function (err, obj) {
            t.ifError(err, 'getObject() error');
            assertObject(t, obj, key, exp);
            cb(err, obj);
        });
    }

    var k1exp;

    vasync.pipeline({ funcs: [
        function (_, cb) {
            vasync.forEachPipeline({
                inputs: ignored,
                func: function (ignore, cb2) {
                    c.putObject(b, ignore, ignobj, cb2);
                }
            }, cb);
        },

        // Trying to update multiple objects to have same unique field fails
        function (_, cb) {
            c.updateObjects(b, changed2, '(str_2=hello)',
                function (err, res) {
                t.ok(err, 'updateObjects() should fail');
                t.notOk(res, 'No result object');
                if (err) {
                    t.ok(VError.hasCauseWithName(err, 'UniqueAttributeError'),
                        'Failed due to unique attribute');
                }
                cb();
            });
        },

        // Field to update isn't indexed, and can't be updated.
        function (_, cb) {
            c.updateObjects(b, { nonexistent: true }, '(str_2=hello)',
                function (err, res) {
                t.ok(err, 'updateObjects() should fail');
                t.notOk(res, 'No result object');
                if (err) {
                    t.ok(VError.hasCauseWithName(err, 'NotIndexedError'),
                        'Failed due to missing index');
                }
                cb();
            });
        },

        // Insert the first object that we'll be affecting.
        function (_, cb) { c.putObject(b, k1, { str_2: v1 }, cb); },

        // updateObjects() should update all fields.
        function (_, cb) {
            k1exp = changed1;
            doUpdate(changed1, '(str_2=' + v1 + ')', 1, cb);
        },
        function (_, cb) { checkObject(k1, k1exp, cb); },

        // updateObjects() with same filter no longer applies; object unchanged.
        function (_, cb) {
            doUpdate(changed2, '(str_2=' + v1 + ')', 0, cb);
        },
        function (_, cb) { checkObject(k1, k1exp, cb); },

        // updateObjects() with new filter should find and update all fields.
        function (_, cb) {
            k1exp = jsprim.mergeObjects(k1exp, changed2);
            doUpdate(changed2, '(str_2=' + v2 + ')', 1, cb);
        },
        function (_, cb) { checkObject(k1, k1exp, cb); },

        // We should be able to make array-type fields empty.
        function (_, cb) {
            k1exp = jsprim.mergeObjects(k1exp, changed3);
            doUpdate(changed3, '(str_2=' + v3 + ')', 1, cb);
        },
        function (_, cb) { checkObject(k1, k1exp, cb); },

        // Add two new objects with the same "str_2" value.
        function (_, cb) { c.putObject(b, k2, { str_2: v3 }, cb); },
        function (_, cb) { c.putObject(b, k3, { str_2: v3 }, cb); },

        // Our filter should now find and update all three.
        function (_, cb) {
            k1exp = jsprim.mergeObjects(k1exp, { str_2: v4 });
            doUpdate({ str_2: v4 }, '(str_2=' + v3 + ')', 3, cb);
        },
        function (_, cb) { checkObject(k1, k1exp, cb); },
        function (_, cb) { checkObject(k2, { str_2: v4 }, cb); },
        function (_, cb) { checkObject(k3, { str_2: v4 }, cb); },

        // The untouched objects should have the same, initial value
        function (_, cb) {
            vasync.forEachPipeline({
                inputs: ignored,
                func: function (key, cb2) {
                    checkObject(key, ignobj, cb2);
                }
            }, cb);
        }
    ] }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('MORAY-166: delete w/LIMIT in batch', function (t) {
    var k = uuid.v4();

    vasync.pipeline({
        funcs: [
            function putObjects(_, cb) {
                cb = once(cb);
                var barrier = vasync.barrier();
                var vals = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
                vals.forEach(function (i) {
                    barrier.start(i);
                    var _k = k + i;
                    var v = {
                        num: i
                    };

                    c.putObject(b, _k, v, function (err) {
                        if (err)
                            cb(err);

                        barrier.done(i);
                    });
                });

                barrier.on('drain', cb);
            },
            function deleteObjects(_, cb) {
                cb = once(cb);
                c.batch([
                    {
                        operation: 'deleteMany',
                        bucket: b,
                        filter: 'num=*',
                        options: {
                            limit: 5
                        }
                    }
                ], function (err, meta) {
                    if (err) {
                        cb(err);
                        return;
                    }
                    t.ok(meta);
                    t.equal(meta.etags[0].count, 5);
                    cb();
                });
            }
        ]
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('MORAY-175: overwrite with \' in name', function (t) {
    var k = uuid.v4() + '\'foo';
    var v = {
        str: 'hi',
        vnode: 2
    };
    var v2 = {
        str: 'hello world',
        pre: 'hi'
    };

    vasync.pipeline({
        funcs: [ function create(_, cb) {
            c.putObject(b, k, v, cb);
        }, function overwrite(_, cb) {
            c.putObject(b, k, v2, cb);
        }, function getAgain(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err) {
                    cb(err);
                } else {
                    t.ok(obj);
                    v2.pre = 'pre_overwrite';
                    assertObject(t, obj, k, v2);
                    cb();
                }
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('reindex objects', function (t) {

    var field = 'unindexed';
    var COUNT = 1000;
    var PAGESIZE = 100;
    var records = [];
    for (var i = 0; i < COUNT; i++) {
        records.push(i);
    }

    vasync.pipeline({
        funcs: [
            function insertRecords(_, cb) {
                vasync.forEachPipeline({
                    func: function (id, callback) {
                        var k = uuid.v4();
                        var obj = {
                            str: 'test'
                        };
                        obj[field] = id;
                        c.putObject(b, k, obj, function (err, meta) {
                            callback(err);
                        });
                    },
                    inputs: records
                }, function (err) {
                    t.ifError(err);
                    t.ok(true, 'insert records');
                    cb(err);
                });
            },
            function updateBucket(_, cb) {
                var config = jsprim.deepCopy(BUCKET_CFG);
                config.index[field] =  {type: 'number'};
                config.options.version++;
                c.updateBucket(b, config, function (err) {
                    t.ifError(err);
                    t.ok(true, 'update bucket');
                    cb(err);
                });
            },
            function reindexObjects(_, cb) {
                var total = 0;
                function runReindex() {
                    c.reindexObjects(b, PAGESIZE, function (err, res) {
                        if (err) {
                            t.ifError(err);
                            cb(err);
                            return;
                        }
                        if (res.processed === 0) {
                            t.equal(COUNT, total);
                            cb();
                        } else {
                            total += res.processed;
                            process.nextTick(runReindex);
                        }
                    });
                }
                runReindex();
            },
            function queryNewIndex(_, cb) {
                var limit = COUNT / 2;
                var filter = util.format('(%s<=%d)', field, limit);

                var found = 0;
                var opts = {
                    noBucketCache: true
                };
                var res = c.findObjects(b, filter, opts);
                res.on('error', cb);
                res.on('record', function () {
                    found++;
                });
                res.on('end', function () {
                    // <= means limit+1
                    t.equal(limit+1, found);
                    cb();
                });
            }
        ]
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});

test('MORAY-291: add ip', function (t) {
    var k = uuid.v4();
    var v = {
        ip: '192.168.1.10'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, function (err, meta) {
                if (err)
                    return (cb(err));

                t.ok(meta);
                if (meta)
                    t.ok(meta.etag);
                return (cb());
            });
        }, function get(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                assertObject(t, obj, k, v);
                t.ok(obj.value.ip, 'has ip value');

                if (obj.value.ip) {
                    t.ok(net.isIPv4(obj.value.ip), 'ip value is IPv4');
                    t.equal(obj.value.ip, v.ip, 'ip is correct');
                }

                return (cb());
            });
        }]
    }, function (err) {
        t.ifError(err, 'no errors');
        t.end();
    });
});

test('MORAY-291: add partial ip not ok', function (t) {
    var k = uuid.v4();
    var v = {
        ip: '192.168'
    };
    var errmsg = 'index(ip) is of type ip';

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, function (err, meta) {
                if (err) {
                    t.ok(err, 'received an error');
                    t.notEqual(err.message.indexOf(errmsg), -1,
                        'with the right message');
                    return (cb());
                }
                t.notOk(false, 'did not error on bogus ip');
                return (cb());
            });
        }]
    }, function (err) {
        t.ifError(err, 'no errors');
        t.end();
    });
});

test('MORAY-291: add ip/cidr not ok', function (t) {
    var k = uuid.v4();
    var v = {
        ip: '192.168.1.10/24'
    };
    var errmsg = 'index(ip) is of type ip';

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, function (err, meta) {
                if (err) {
                    t.ok(err, 'received an error');
                    t.notEqual(err.message.indexOf(errmsg), -1,
                        'with the right message');
                    return (cb());
                }
                t.notOk(false, 'did not error on ip/cidr input');
                return (cb());
            });
        }]
    }, function (err) {
        t.ifError(err, 'no errors');
        t.end();
    });
});

test('MORAY-291: add subnet', function (t) {
    var k = uuid.v4();
    var v = {
        subnet: '192.168.1.0/24'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, function (err, meta) {
                if (err)
                    return (cb(err));

                t.ok(meta);
                if (meta)
                    t.ok(meta.etag);
                return (cb());
            });
        }, function get(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                t.ok(obj.value.subnet, 'has subnet value');

                if (obj.value.ip) {
                    t.equal(obj.value.subnet, v.subnet, 'subnet value correct');
                }

                return (cb());
            });
        }]
    }, function (err) {
        t.ifError(err, 'no errors');
        t.end();
    });
});

test('MORAY-291: invalid subnet', function (t) {
    var k = uuid.v4();
    var v = {
        subnet: '192.168.1.10/24'
    };
    var errmsg = 'invalid cidr value: "' + v.subnet + '"';

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, function (err, meta) {
                if (err) {
                    t.ok(err, 'received an error');
                    t.ok(jsprim.endsWith(err.message, errmsg,
                        'with the right message'));
                    return (cb());
                }
                t.notOk(false, 'did not error on bogus ip');
                return (cb());
            });
        }]
    }, function (err) {
        t.ifError(err, 'no errors');
        t.end();
    });
});

test('MORAY-333: able to query on null subnet field', function (t) {
    var k = uuid.v4();
    var v = {
        ip: '192.168.1.10'
    };

    vasync.pipeline({
        funcs: [
            function put(_, cb) {
                c.putObject(b, k, v, function (err, meta) {
                    if (err)
                        return (cb(err));

                    t.ok(meta);
                    if (meta)
                        t.ok(meta.etag);
                    return (cb());
                });
            },
            function query(_, cb) {
                var f = '(|(subnet=10.0.0.0/8)(ip=192.168.1.10))';
                var req = c.findObjects(b, f);
                var ok = false;
                req.once('error', function (err) {
                    t.ifError(err, 'query error');
                    t.end();
                });
                req.once('end', function () {
                    t.ok(ok);
                    t.end();
                });
                req.on('record', function (obj) {
                    t.ok(obj, 'received an object from the query');
                    assertObject(t, obj, k, v);
                    ok = true;
                });
            }
        ]
    }, function (err) {
        t.ifError(err, 'no errors');
        t.end();
    });
});

test('MORAY-333: able to query on null IP field', function (t) {
    var k = uuid.v4();
    var v = {
        subnet: '192.168.0.0/16'
    };

    vasync.pipeline({
        funcs: [
            function put(_, cb) {
                c.putObject(b, k, v, function (err, meta) {
                    if (err)
                        return (cb(err));

                    t.ok(meta);
                    if (meta)
                        t.ok(meta.etag);
                    return (cb());
                });
            },
            function query(_, cb) {
                var f = '(|(ip=1.2.3.4)(subnet=192.168.0.0/16))';
                var req = c.findObjects(b, f);
                var ok = false;
                req.once('error', function (err) {
                    t.ifError(err, 'query error');
                    t.end();
                });
                req.once('end', function () {
                    t.ok(ok);
                    t.end();
                });
                req.on('record', function (obj) {
                    t.ok(obj, 'received an object from the query');
                    assertObject(t, obj, k, v);
                    ok = true;
                });
            }
        ]
    }, function (err) {
        t.ifError(err, 'no errors');
        t.end();
    });
});


// TODO: should create own bucket.
test('MORAY-291: able to query on IP types', function (t) {
    var k = uuid.v4();
    var v = {
        ip: '192.168.1.10'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, function (err, meta) {
                if (err)
                    return (cb(err));

                t.ok(meta);
                if (meta)
                    t.ok(meta.etag);
                return (cb());
            });
        }, function query(_, cb) {
            var f = '(ip=192.168.1.10)';
            var req = c.findObjects(b, f);
            var ok = false;
            req.once('error', function (err) {
                t.ifError(err, 'query error');
                t.end();
            });
            req.once('end', function () {
                t.ok(ok);
                t.end();
            });
            req.on('record', function (obj) {
                t.ok(obj, 'received an object from the query');
                assertObject(t, obj, k, v);
                ok = true;
            });
        }]
    }, function (err) {
        t.ifError(err, 'no errors');
        t.end();
    });
});

// TODO: should create own bucket.
test('MORAY-291: able to query <= on IP types', function (t) {
    var k = uuid.v4();
    var v = {
        ip: '192.168.1.8'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, function (err, meta) {
                if (err)
                    return (cb(err));

                t.ok(meta);
                if (meta)
                    t.ok(meta.etag);
                return (cb());
            });
        }, function query(_, cb) {
            var f = '(ip<=192.168.1.9)';
            var req = c.findObjects(b, f);
            var ok = false;
            req.once('error', function (err) {
                t.ifError(err, 'query error');
                t.end();
            });
            req.once('end', function () {
                t.ok(ok);
                t.end();
            });
            req.on('record', function (obj) {
                t.ok(obj, 'received an object from the query');
                assertObject(t, obj, k, v);
                t.ok(obj.value.ip, 'has ip value');

                if (obj.value.ip) {
                    t.ok(net.isIPv4(obj.value.ip), 'ip value is IPv4');
                    t.equal(obj.value.ip, v.ip, 'ip is correct');
                }

                ok = true;
            });
        }]
    }, function (err) {
        t.ifError(err, 'no errors');
        t.end();
    });
});

// TODO: other queries on IP types that we need: <=
// TODO: queries on subnet types =, <=

test('MORAY-298: presence filter works for all types', function (t) {
    var recs = [
        {
            k: 'date',
            v: '1989-11-09T21:43:56.987Z'
        },
        {
            k: 'date_a',
            v: [ '1961-08-13T00:11:22.333Z' ]
        },
        {
            k: 'date_u',
            v: '1961-08-13T00:11:22.333Z'
        },
        {
            k: 'dater',
            v: '[1776-07-04T00:00:00.000Z,)'
        },
        {
            k: 'dater_u',
            v: '[1492-12-25T00:00:00.000Z,)'
        },
        {
            k: 'str',
            v: 'string'
        },
        {
            k: 'str_a',
            v: [ 'my string' ]
        },
        {
            k: 'str_u',
            v: 'unique string'
        },
        {
            k: 'num',
            v: 40
        },
        {
            k: 'num_a',
            v: [ 75.75 ]
        },
        {
            k: 'num_u',
            v: 50.521
        },
        {
            k: 'numr',
            v: '[18,)'
        },
        {
            k: 'numr_u',
            v: '[21,)'
        },
        {
            k: 'bool',
            v: true
        },
        {
            k: 'bool_a',
            v: [ true, false, true ]
        },
        {
            k: 'bool_u',
            v: true
        },
        {
            k: 'ip',
            v: '192.168.5.2'
        },
        {
            k: 'ip_a',
            v: [ '10.10.10.25', '10.50.20.35', '10.75.35.30' ]
        },
        {
            k: 'ip_u',
            v: '192.168.5.3'
        },
        {
            k: 'mac',
            v: '52:fa:1e:e3:ef:9b'
        },
        {
            k: 'mac_a',
            v: [ '32:ed:64:95:c0:b3', '4d:c8:5e:c8:56:d3' ]
        },
        {
            k: 'mac_u',
            v: '44:bc:b2:10:15:87'
        },
        {
            k: 'subnet',
            v: '192.168.5.0/24'
        },
        {
            k: 'subnet_a',
            v: [ '192.168.4.0/24', '192.168.5.0/24', '192.168.6.0/24' ]
        },
        {
            k: 'subnet_u',
            v: '192.168.6.0/24'
        },
        {
            k: 'uuid',
            v: '9f100df6-43e8-64fd-f109-ba9cfbec8cdb'
        },
        {
            k: 'uuid_u',
            v: '4201e559-174b-c756-9cfc-fdd1e2ff92fe'
        }
    ];

    t.equal(recs.length, INDEXES.length - 1, 'all fields tested');

    vasync.forEachParallel({
        inputs: recs,
        func: function presence(rec, cb) {
            var v = {};
            v[rec.k] = rec.v;

            c.putObject(b, rec.k, v, function (putErr, meta) {
                var desc = ': ' + rec.k + '/' + rec.v;
                var f = util.format('(%s=*)', rec.k);
                var n = 0;
                var req;

                t.ifErr(putErr, 'put' + desc);
                if (putErr)
                    return (cb(putErr));

                req = c.findObjects(b, f);

                req.once('error', function (err) {
                    t.ifError(err, 'query error' + desc);
                    return (cb(err));
                });

                req.once('end', function () {
                    t.equal(n, 1, '1 record returned' + desc);
                    return (cb());
                });

                req.on('record', function (obj) {
                    n++;
                    t.deepEqual(obj.value[rec.k], rec.v, 'value' + desc);
                });

                return req;
            });
        }
    }, function (err) {
        t.ifError(err, 'no errors');
        t.end();
    });
});

test('filter on unindexed fields', function (t) {
    var v = {
        str: 'required',
        ui_str: 'value',
        ui_num: 15,
        ui_zero: 0,
        ui_null: null
    };
    var k = uuid.v4();
    var tests = {
        // Equality:
        '(ui_str=value)': true,
        '(ui_str=bad)': false,
        // '(ui_num=15)': true, ruined by strict types
        '(ui_num=14)': false,
        '(ui_num=0)': false,
        // '(ui_zero=0)': true, ruined by strict types
        '(ui_zero=1)': false,
        // Presence:
        '(ui_str=*)': true,
        '(ui_num=*)': true,
        '(ui_zero=*)': true,
        '(ui_null=*)': false,
        '(ui_bogus=*)': false,
        // GE/LE:
        '(ui_num>=15)': true,
        '(ui_num>=0)': true,
        '(ui_num>=16)': false,
        '(ui_num<=15)': true,
        '(ui_num<=0)': false,
        '(ui_num<=16)': true,
        '(ui_str>=value)': true,
        '(ui_str>=valud)': true,
        '(ui_str>=valuf)': false,
        '(ui_str<=value)': true,
        '(ui_str<=valud)': false,
        '(ui_str<=valuf)': true,
        // Substring:
        '(ui_str=val*)': true,
        '(ui_str=val*e)': true,
        '(ui_str=*alue)': true,
        '(ui_str=v*l*e)': true,
        '(ui_str=n*ope)': false,
        '(ui_str=*nope)': false,
        '(ui_str=nope*)': false,
        '(ui_str=no*p*e)': false,
        // Ext:
        '(ui_str:caseIgnoreMatch:=VALUE)': true,
        '(ui_str:caseIgnoreMatch:=NOPE)': false,
        '(ui_str:caseIgnoreSubstringsMatch:=V*LUE)': true,
        '(ui_str:caseIgnoreSubstringsMatch:=N*PE)': false
    };
    c.putObject(b, k, v, function (putErr) {
        if (putErr) {
            t.ifError(putErr);
            t.end();
            return;
        }
        vasync.forEachParallel({
            inputs: Object.keys(tests),
            func: function filterCheck(f, cb) {
                var found = false;
                cb = once(cb);
                var fixed = '(&(str=required)' + f + ')';
                var res = c.findObjects(b, fixed);
                res.once('error', function (err) {
                    t.ifError(err);
                    cb(err);
                });
                res.on('record', function (obj) {
                    if (k !== obj.key)
                        t.fail('invalid key');
                    found = true;
                });
                res.once('end', function () {
                    if (tests[f]) {
                        t.ok(found, f + ' should find object');
                    } else {
                        t.notOk(found, f + ' should not find object');
                    }
                    cb();
                });
            }
        }, function (err) {
            t.ifError(err);
            t.end();
        });
    });
});

test('MORAY-311: ext filters survive undefined fields', function (t) {
    var v = {
        num: 5
    };
    var k = uuid.v4();
    var filters = [
        '(&(num=5)(!(str:caseIgnoreSubstringsMatch:=*test*)))',
        '(&(num=5)(!(str:caseIgnoreMatch:=*test*)))'
    ];
    c.putObject(b, k, v, function (putErr) {
        if (putErr) {
            t.ifError(putErr);
            t.end();
            return;
        }
        vasync.forEachParallel({
            inputs: filters,
            func: function filterCheck(f, cb) {
                var found = false;
                cb = once(cb);
                var res = c.findObjects(b, f);
                res.once('error', function (err) {
                    t.ifError(err);
                    cb(err);
                });
                res.on('record', function (obj) {
                    t.equal(k, obj.key);
                    found = true;
                });
                res.once('end', function () {
                    t.ok(found);
                    cb();
                });
            }
        }, function (err) {
            t.ifError(err);
            t.end();
        });
    });
});

test('UniqueAttributeError for every type', function (t) {
    var conflict = {
        date_u: '2015-10-21T00:00:00.000Z',
        dater_u: '(2015-10-21T00:00:00.000Z,)',
        str_u: 'hello',
        num_u: 40,
        numr_u: '(9000,)',
        bool_u: true,
        ip_u: '1.2.3.4',
        mac_u: '58:57:1a:d1:42:a1',
        subnet_u: '10.0.0.0/8',
        uuid_u: '725f6c18-12de-c9ef-fb37-d3d14ba3d24f'
    };

    function checkConflicts(_, cb) {
        function checkConflict(attr, cb2) {
            var o = {};
            o[attr] = conflict[attr];

            c.putObject(b, uuid.v4(), o, function (err) {
                var msg =
                    fmt('putObject(%j) fails with UniqueAttributeError', o);

                if (err) {
                    if (VError.hasCauseWithName(err, 'UniqueAttributeError')) {
                        t.pass(msg);
                    } else {
                        t.ifError(err, msg);
                    }
                } else {
                    t.fail(msg);
                }

                cb2();
            });
        }

        vasync.forEachPipeline({
            inputs: Object.keys(conflict),
            func: checkConflict
        }, cb);
    }

    function noConflict(attr, value) {
        return function (_, cb) {
            var k = uuid.v4();
            var o = {};
            o[attr] = value;

            c.putObject(b, k, o, function (pErr) {
                if (pErr) {
                    t.ifError(pErr, 'putObject() error');
                    cb();
                    return;
                }

                c.getObject(b, k, function (gErr, mo) {
                    t.ifError(gErr, 'getObject() error');
                    assertObject(t, mo, k, o);
                    cb();
                });
            });
        };
    }

    t.equal(Object.keys(conflict).length, NUM_UNIQUE_INDEXES,
        'all unique fields tested');

    vasync.pipeline({
        funcs: [
            function (_, cb) {
                c.putObject(b, uuid.v4(), conflict, cb);
            },

            checkConflicts,

            /*
             * Check that values that are similar, but not the same, can be
             * loaded into the bucket.
             */
            noConflict('date_u', '2015-10-21T00:00:00.001Z'),
            noConflict('dater_u', '(2015-10-21T00:00:00.001Z,)'),
            noConflict('str_u', 'Hello'),
            noConflict('num_u', 40.1),
            noConflict('numr_u', '(,9000)'),
            noConflict('numr_u', '(9001,)'),
            noConflict('bool_u', false),
            noConflict('ip_u', '1.2.3.5'),
            noConflict('mac_u', '38:7c:0e:5d:bf:9b'),
            noConflict('subnet_u', '10.0.0.0/24'),
            noConflict('uuid_u', '725f6c18-12de-c9ef-fb37-d3d14ba3d24e')
        ]
    }, function (err) {
        t.ifError(err);
        t.end();
    });

});

test('InvalidIndexTypeError/InvalidQueryError for every type', function (t) {
    /*
     * For the array-type columns, place the bad values at the end of the array,
     * so that it can be found by the InvalidQueryError tests below. (There's
     * also no point in testing invalid values that are no in an array, since
     * Moray implicitly wraps singleton values.)
     */
    var tests = [
        {
            attr: 'date',
            values: [
                'foo',
                'August 1, 2020',
                '1970-01-01T00:00:00.000',
                '1970-01-01 00:00:00.000Z',
                '-1970-01-01T00:00:00.000Z',
                '1970-01-01',
                0,
                1529346379184
            ]
        },
        {
            attr: 'date_a',
            values: [
                [ '1970-01-01T00:00:00.000Z', 'Thursday, 1 January 1970' ],
                [ 0 ],
                [ 1529346379184 ]
            ]
        },
        {
            attr: 'dater',
            values: [
                '{,}',
                '[,foo]',
                '[foo,]'
            ]
        },
        {
            attr: 'numr',
            values: [
                '{,}',
                '[,foo]',
                '[foo,]',
                '[,2.foo]',
                '[2.foo,]'
            ]
        },
        {
            attr: 'bool',
            values: [
                'foo',
                'truef',
                'troo',
                'tf',
                'ft',
                23
            ]
        },
        {
            attr: 'bool_a',
            values: [
                [ true, false, true, 'bar' ]
            ]
        },
        {
            attr: 'ip',
            values: [
                'foo',
                '1.2.3.4.5',
                '1.2.3',
                'fd00:::1'
            ]
        },
        {
            attr: 'ip_a',
            values: [
                [ '1.2.3.4', 'fd00::1', 'foo' ]
            ]
        },
        {
            attr: 'mac',
            values: [
                'foo',
                true,
                2012543500901123,
                '33:06:49:0f:4b:3q',
                '333:06:49:0f:4b:3b',
                '33:006:49:0f:4b:3b',
                '06:49:0f:4b:3b',
                'ab:33:06:49:0f:4b:3b'
            ]
        },
        {
            attr: 'mac_a',
            values: [
                [ '6a:94:d5:ab:54:ba', 'foo' ]
            ]
        },
        {
            attr: 'subnet',
            values: [
                'foo',
                '1.2.3.0/40',
                'fd00::/-20',
                'fd00::/150',
                'fd00:::/64'
            ]
        },
        {
            attr: 'subnet_a',
            values: [
                [ 'fd00::/64', 'foo' ]
            ]
        },
        {
            attr: 'uuid',
            values: [
                'foo',
                '0123',
                'abcd',
                '5ec7875-bd10-401a-cfc1-dc1e90018abe',
                '5ec78759-bd1-401a-cfc1-dc1e90018abe',
                '5ec78759-bd10-401-cfc1-dc1e90018abe',
                '5ec78759-bd10-401a-cfc-dc1e90018abe',
                '5ec78759-bd10-401a-cfc1-dc1e90018ab',
                '1-5ec78759-bd10-401a-cfc1-dc1e90018abe'
            ]
        },

        /*
         * Unfortunately, Moray is extremely accepting for string type columns,
         * and some services rely on this. (For example, UFDS sends arrays and
         * expects to be stringified such that its contents are separated by a
         * comma.) We just leave empty "values" arrays here, and hopefully can
         * someday tighten things up.
         */
        {
            attr: 'str',
            values: [ ]
        },
        {
            attr: 'str_a',
            values: [ ]
        },
        {
            attr: 'str2',
            values: [ ]
        },
        /*
         * And, naturally, the same goes for number type columns! Workflow sends
         * strings that get turned into NaN, which get turned into NULL, and
         * UFDS sends input like [ '4' ], which parseInt()/parseFloat() will
         * happily coerce into a string and then parse.
         */
        {
            attr: 'num',
            values: [ ]
        },
        {
            attr: 'num_a',
            values: [ ]
        }
    ];

    t.equal(tests.length, NUM_NON_UNIQUE_INDEXES, 'all fields tested');

    function tryKV(t2, attr, value, cb) {
        var o = {};
        o[attr] = value;

        c.putObject(b, uuid.v4(), o, function (err) {
            var msg = fmt('putObject(%j) fails with InvalidIndexTypeError', o);

            if (err) {
                if (VError.hasCauseWithName(err, 'InvalidIndexTypeError')) {
                    t2.pass(msg);
                } else {
                    t2.ifError(err, msg);
                }
            } else {
                t2.fail(msg);
            }

            cb();
        });
    }

    function tryQuery(t2, attr, value, cb) {
        var filter = fmt('(%s=%s)', attr, value);
        var msg = fmt('findObjects(%j) fails w/ InvalidQueryError', filter);
        var res = c.findObjects(b, filter);

        res.on('record', function (row) {
            t2.deepEqual(row, null, msg);
        });

        res.on('error', function (err) {
            if (VError.hasCauseWithName(err, 'InvalidQueryError')) {
                t2.pass(msg);
            } else {
                t2.ifError(err, msg);
            }

            cb();
        });

        res.on('end', function () {
            t2.fail(msg);
            cb();
        });
    }

    tests.forEach(function (info) {
        if (info.values.length === 0) {
            return;
        }

        t.test(fmt('InvalidIndexTypeError for %j', info.attr), function (t2) {
            vasync.forEachPipeline({
                inputs: info.values,
                func: function (value, cb) {
                    tryKV(t2, info.attr, value, cb);
                }
            }, function (err) {
                t2.ifError(err, info.attr + ' tests error');
                t2.end();
            });
        });

        t.test(fmt('InvalidQueryError for %j', info.attr), function (t2) {
            vasync.forEachPipeline({
                inputs: info.values,
                func: function (value, cb) {
                    if (Array.isArray(value)) {
                        value = value[value.length - 1];
                    }

                    tryQuery(t2, info.attr, value, cb);
                }
            }, function (err) {
                t2.ifError(err, info.attr + ' tests error');
                t2.end();
            });
        });
    });
});
