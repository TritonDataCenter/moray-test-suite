/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var fmt = require('util').format;
var once = require('once');
var tape = require('tape');
var libuuid = require('libuuid');
var vasync = require('vasync');
var VError = require('verror');

var helper = require('./helper.js');

// --- Globals

var uuid = {
    v1: libuuid.create,
    v4: libuuid.create
};

var c; // client
var server;
var b; // bucket

var BUCKET_CFG = {
    index: {
        mac: {
            type: 'mac'
        },
        ip: {
            type: 'ip'
        },
        ip_a: {
            type: '[ip]'
        },
        subnet: {
            type: 'subnet'
        },
        subnet_a: {
            type: '[subnet]'
        }
    }
};

function test(name, setup) {
    tape.test(name + ' - setup', function (t) {
        b = 'moray_unit_test_' + uuid.v4().substr(0, 7);
        helper.createServer(null, function (s) {
            server = s;
            c = helper.createClient();
            c.on('connect', t.end.bind(t));
        });
    });

    tape.test(name + ' - main', setup);

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

// --- Tests

test('MAC addresses - findObjects()', function (t) {
    var key1 = uuid.v4();
    var obj1 = {
        mac: '04:31:f4:15:28:ec'
    };

    var key2 = uuid.v4();
    var obj2 = {
        mac: '1f:10:76:e3:7a:bc'
    };

    var key3 = uuid.v4();
    var obj3 = {
        mac: '47:33:47:03:7a:b0'
    };

    var key4 = uuid.v4();
    var obj4 = {
        mac: '6f:4d:67:cd:42:9c'
    };

    var key5 = uuid.v4();
    var obj5 = {
        mac: 'af:4d:67:cd:42:9c'
    };

    var ALL = [ obj1, obj2, obj3, obj4, obj5 ];

    function put(k, o) {
        return function (_, cb) {
            c.putObject(b, k, o, cb);
        };
    }

    function check(f, errnom) {
        return function (_, cb) {
            var msg = fmt('findObjects(%j) fails w/ %s', f, errnom);
            var res = c.findObjects(b, f);

            res.on('record', function (row) {
                t.deepEqual(row, null, msg);
            });

            res.on('error', function (err) {
                if (VError.hasCauseWithName(err, errnom)) {
                    t.pass(msg);
                } else {
                    t.ifError(err, msg);
                }

                cb();
            });

            res.on('end', function () {
                t.fail(msg);
                cb();
            });
        };
    }

    function inv(f) {
        return check(f, 'InvalidQueryError');
    }

    function expect(f, os) {
        return function (_, cb) {
            var res = c.findObjects(b, f, {
                sort: {
                    attribute: 'mac'
                }
            });

            var rows = [];
            res.on('record', function (row) {
                rows.push(row.value);
            });

            res.on('error', function (err) {
                t.ifError(err, fmt('findObjects(%j)', f));
                cb();
            });

            res.on('end', function () {
                t.deepEqual(rows, os, fmt('findObjects(%j) results', f));
                cb();
            });
        };
    }

    vasync.pipeline({
        funcs: [
            function bucket(_, cb) {
                c.putBucket(b, BUCKET_CFG, cb);
            },

            put(key1, obj1),
            put(key2, obj2),
            put(key3, obj3),
            put(key4, obj4),
            put(key5, obj5),

            // equality tests
            expect('(mac=04:31:f4:15:28:ec)', [ obj1 ]),
            expect('(mac=1f:10:76:e3:7a:bc)', [ obj2 ]),
            expect('(mac=47:33:47:03:7a:b0)', [ obj3 ]),
            expect('(mac=6f:4d:67:cd:42:9c)', [ obj4 ]),
            expect('(mac=af:4d:67:cd:42:9c)', [ obj5 ]),

            // not equal tests
            expect('!(mac=04:31:f4:15:28:ec)', [ obj2, obj3, obj4, obj5 ]),
            expect('!(mac=1f:10:76:e3:7a:bc)', [ obj1, obj3, obj4, obj5 ]),
            expect('!(mac=47:33:47:03:7a:b0)', [ obj1, obj2, obj4, obj5 ]),
            expect('!(mac=6f:4d:67:cd:42:9c)', [ obj1, obj2, obj3, obj5 ]),
            expect('!(mac=af:4d:67:cd:42:9c)', [ obj1, obj2, obj3, obj4 ]),

            // greater/eq tests
            expect('(mac>=2c:23:ad:de:ec:6b)', [ obj3, obj4, obj5 ]),
            expect('(mac>=1f:10:76:e3:7a:bc)', [ obj2, obj3, obj4, obj5 ]),

            // lesser/eq tests
            expect('(mac<=2c:23:ad:de:ec:6b)', [ obj1, obj2 ]),
            expect('(mac<=6f:4d:67:cd:42:9c)', [ obj1, obj2, obj3, obj4 ]),

            // fetch everything
            expect('(mac=*)', ALL),
            expect('(mac>=00:00:00:00:00:00)', ALL),
            expect('(mac<=ff:ff:ff:ff:ff:ff)', ALL),

            // invalid values
            inv('(mac=foo)'),
            inv('(mac<=foo)'),
            inv('(mac>=foo)'),
            inv('(mac=3q:1c:c0:8b:09:b7)'),
            inv('(mac<=3q:1c:c0:8b:09:b7)'),
            inv('(mac>=3q:1c:c0:8b:09:b7)'),
            inv('(mac=1c:c0:8b:09:b7)'),
            inv('(mac<=1c:c0:8b:09:b7)'),
            inv('(mac>=1c:c0:8b:09:b7)')
        ],
        arg: null
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});

test('Network ranges - invalid filters', function (t) {
    function check(attr, ext, val) {
        var f = fmt('(%s:%s:=%s)', attr, ext, val);

        return function (_, cb) {
            var msg = fmt('findObjects(%j) fails w/ InvalidQueryError', f);
            var res = c.findObjects(b, f);

            res.on('record', function (row) {
                t.deepEqual(row, null, msg);
            });

            res.on('error', function (err) {
                if (VError.hasCauseWithName(err, 'InvalidQueryError')) {
                    t.pass(msg);
                } else {
                    t.ifError(err, msg);
                }

                cb();
            });

            res.on('end', function () {
                t.fail(msg);
                cb();
            });
        };
    }

    function genTests(ip, subnet) {
        return [
            // invalid "within" tests
            check(ip, 'within', '5'),
            check(ip, 'within', '1.2.3/24'),
            check(ip, 'within', '300.0.0.0/8'),
            check(ip, 'within', '1.2.3.0/240'),
            check(ip, 'within', '1.2.3.0/24x'),

            check(subnet, 'within', '1.2.3.0/24'),
            check(subnet, 'within', 'fd00::/64'),

            // invalid "contains" tests
            check(subnet, 'contains', 'foo)'),
            check(subnet, 'contains', 'true)'),
            check(subnet, 'contains', '1.2.3.300)'),
            check(subnet, 'contains', '1.2.3.4.5)'),
            check(subnet, 'contains', 'fd00::xxx)'),
            check(subnet, 'contains', '5)'),

            check(ip, 'contains', '1.2.3.4'),
            check(ip, 'contains', 'fd00::1')
        ];
    }

    vasync.pipeline({
        funcs: [
            function bucket(_, cb) {
                c.putBucket(b, BUCKET_CFG, cb);
            }
        ].concat(genTests('ip', 'subnet'), genTests('ip_a', 'subnet_a')),
        arg: null
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});

test('Network ranges - findObjects()', function (t) {
    var key1 = uuid.v4();
    var obj1 = {
        ip: '10.1.3.5',
        ip_a: [ '10.1.3.5' ],
        subnet: '10.1.3.0/24',
        subnet_a: [ '10.1.3.0/24' ]
    };

    var key2 = uuid.v4();
    var obj2 = {
        ip: '192.168.40.234',
        ip_a: [ '192.168.40.234' ],
        subnet: '192.168.0.0/16',
        subnet_a: [ '192.168.0.0/16' ]
    };


    var key3 = uuid.v4();
    var obj3 = {
        ip: '::ffff:172.16.0.5',
        ip_a: [ '::ffff:172.16.0.5' ],
        subnet: '::ffff:172.16.0.0/112',
        subnet_a: [ '::ffff:172.16.0.0/112' ]
    };

    var key4 = uuid.v4();
    var obj4 = {
        ip: 'fd00::321',
        ip_a: [ 'fd00::321' ],
        subnet: 'fd00::/64',
        subnet_a: [ 'fd00::/64' ]
    };

    var key5 = uuid.v4();
    var obj5 = {
        ip: 'fe80::92e2:baff:fe07:b60',
        ip_a: [ 'fe80::92e2:baff:fe07:b60' ],
        subnet: 'fe80::/10',
        subnet_a: [ 'fe80::/10' ]
    };

    function put(k, o) {
        return function (_, cb) {
            c.putObject(b, k, o, cb);
        };
    }

    function expect(attr, ext, val, os) {
        var f = fmt('(%s:%s:=%s)', attr, ext, val);

        return function (_, cb) {
            var res = c.findObjects(b, f, {
                sort: {
                    attribute: 'ip'
                }
            });

            var rows = [];
            res.on('record', function (row) {
                rows.push(row.value);
            });

            res.on('error', function (err) {
                t.ifError(err, fmt('findObjects(%j)', f));
                cb();
            });

            res.on('end', function () {
                t.deepEqual(rows, os, fmt('findObjects(%j) results', f));
                cb();
            });
        };
    }

    function genTests(ip, subnet) {
        return [
            // "within" tests
            expect(ip, 'within', '0.0.0.0/0', [ obj1, obj2 ]),
            expect(ip, 'within', '10.0.0.0/8', [ obj1 ]),
            expect(ip, 'within', '10.1.0.0/16', [ obj1 ]),
            expect(ip, 'within', '10.1.3.0/24', [ obj1 ]),
            expect(ip, 'within', '10.1.4.0/24', [ ]),
            expect(ip, 'within', '192.0.0.0/8', [ obj2 ]),
            expect(ip, 'within', '192.168.0.0/16', [ obj2 ]),
            expect(ip, 'within', '192.168.40.0/24', [ obj2 ]),
            expect(ip, 'within', '192.168.1.0/24', [ ]),
            expect(ip, 'within', '::ffff:0.0.0.0/96', [ obj3 ]),
            expect(ip, 'within', '::ffff:172.16.0.0/112', [ obj3 ]),
            expect(ip, 'within', '::ffff:172.16.0.0/120', [ obj3 ]),
            expect(ip, 'within', 'fd00::/64', [ obj4 ]),
            expect(ip, 'within', 'fd00::0000/64', [ obj4 ]),
            expect(ip, 'within', 'fe80::/10', [ obj5 ]),
            expect(ip, 'within', 'fe80::0000/10', [ obj5 ]),
            expect(ip, 'within', '::/0', [ obj3, obj4, obj5 ]),
            expect(ip, 'within', '0::/0', [ obj3, obj4, obj5 ]),
            expect(ip, 'within', '0::0/0', [ obj3, obj4, obj5 ]),

            // "contains" tests
            expect(subnet, 'contains', '10.1.3.0', [ obj1 ]),
            expect(subnet, 'contains', '10.1.3.20', [ obj1 ]),
            expect(subnet, 'contains', '10.1.3.255', [ obj1 ]),
            expect(subnet, 'contains', '10.1.4.0', [ ]),
            expect(subnet, 'contains', '192.168.20.30', [ obj2 ]),
            expect(subnet, 'contains', '192.168.1.3', [ obj2 ]),
            expect(subnet, 'contains', '192.168.255.255', [ obj2 ]),
            expect(subnet, 'contains', '192.169.0.0', [ ]),
            expect(subnet, 'contains', '::ffff:172.16.5.4', [ obj3 ]),
            expect(subnet, 'contains', '::ffff:172.16.250.20', [ obj3 ]),
            expect(subnet, 'contains', 'fd00::50:20', [ obj4 ]),
            expect(subnet, 'contains', 'fd00::abcd:1234', [ obj4 ]),
            expect(subnet, 'contains', 'fd00::1', [ obj4 ]),
            expect(subnet, 'contains', 'fd00::01', [ obj4 ]),
            expect(subnet, 'contains', 'fd00::001', [ obj4 ]),
            expect(subnet, 'contains', 'fd00::0001', [ obj4 ]),
            expect(subnet, 'contains', 'fe80::8:20ff:fe3e:27b0', [ obj5 ]),
            expect(subnet, 'contains', 'fe80::92b8:d0ff:feb3:8b45', [ obj5 ])
        ];
    }

    vasync.pipeline({
        funcs: [
            function bucket(_, cb) {
                c.putBucket(b, BUCKET_CFG, cb);
            },

            put(key1, obj1),
            put(key2, obj2),
            put(key3, obj3),
            put(key4, obj4),
            put(key5, obj5)
        ].concat(genTests('ip', 'subnet'), genTests('ip_a', 'subnet_a')),
        arg: null
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});
