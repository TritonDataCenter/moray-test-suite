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

// XXX: Need some tests that return no results

var c; // client
var server;
var b; // bucket

var BUCKET_CFG = {
    index: {
        date: {
            type: 'date'
        },
        daterange: {
            type: 'daterange'
        },
        num: {
            type: 'number'
        },
        numrange: {
            type: 'numrange'
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

test('Ranges - invalid filters', function (t) {
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

    function noidx(f) {
        return check(f, 'NotIndexedError');
    }

    vasync.pipeline({
        funcs: [
            function bucket(_, cb) {
                c.putBucket(b, BUCKET_CFG, cb);
            },

            /* BEGIN JSSTYLED */

            // invalid "within" tests
            inv('(num:within:=5)'),
            inv('(num:within:=foo)'),
            inv('(num:within:=true)'),
            inv('(num:within:={5,6})'),
            inv('(num:within:={5,6])'),
            inv('(num:within:=[foo,bar])'),

            inv('(date:within:=5)'),
            inv('(date:within:=foo)'),
            inv('(date:within:=true)'),
            inv('(date:within:={5,6})'),
            inv('(date:within:={5,6])'),
            inv('(date:within:=[foo,bar])'),

            inv('(numrange:within:=[,])'),
            inv('(daterange:within:=[,])'),

            // invalid "contains" tests

            /*
             * It would be great if we could make the validation here strict,
             * but currently the number parsing needs to ignore some bad input.
             * Making this part strict would be nice though:
             *
             * inv('(numrange:contains:=l5)'),
             * inv('(numrange:contains:=foo)'),
             * inv('(numrange:contains:=true)'),
             * inv('(numrange:contains:=[5,6])'),
             * inv('(numrange:contains:={5,6})'),
             * inv('(numrange:contains:={5,6])'),
             * inv('(numrange:contains:=[foo,bar])'),
             */

            inv('(daterange:contains:=[2016-06-05T16:23:04.776Z,2017-06-05T16:23:04.776Z])'),
            inv('(daterange:contains:=foo)'),
            inv('(daterange:contains:=true)'),
            inv('(daterange:contains:=[foo,bar])'),
            inv('(daterange:contains:=1758-05-06T00:00:00.000)'),
            inv('(daterange:contains:=1758-05-06 00:00:00.000Z)'),

            inv('(num:contains:=5)'),
            inv('(date:contains:=1993-05-06T00:00:00.000Z)'),

            // invalid "overlaps" tests
            inv('(numrange:overlaps:=5l)'),
            inv('(numrange:overlaps:=l5)'),
            inv('(numrange:overlaps:=foo)'),
            inv('(numrange:overlaps:=true)'),
            inv('(numrange:overlaps:={5,6})'),
            inv('(numrange:overlaps:={5,6])'),
            inv('(numrange:overlaps:=[foo,bar])'),

            inv('(daterange:overlaps:=5l)'),
            inv('(daterange:overlaps:=l5)'),
            inv('(daterange:overlaps:=foo)'),
            inv('(daterange:overlaps:=true)'),
            inv('(daterange:overlaps:={2016-06-05T16:23:04.776Z,2016-06-05T16:23:04.776Z})'),
            inv('(daterange:overlaps:={2016-06-05T16:23:04.776Z,2016-06-05T16:23:04.776Z])'),
            inv('(daterange:overlaps:=[foo,bar])'),

            inv('(num:overlaps:=[,])'),
            inv('(date:overlaps:=[,])'),

            // no indexes for these attributes
            noidx('nonum:overlaps:=[1,2]'),
            noidx('nonum:within:=[1,2]'),
            noidx('nonumrange:contains:=20')

            /* END JSSTYLED */
        ],
        arg: null
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('Numeric ranges - findObjects()', function (t) {
    var key1 = uuid.v4();
    var obj1 = {
        num: -20,
        numrange: '[-15,-5]'
    };

    var key2 = uuid.v4();
    var obj2 = {
        num: -10,
        numrange: '(5,15)'
    };

    var key3 = uuid.v4();
    var obj3 = {
        num: 10,
        numrange: '(,15]'
    };

    var key4 = uuid.v4();
    var obj4 = {
        num: 20,
        numrange: '[5,)'
    };

    function put(k, o) {
        return function (_, cb) {
            c.putObject(b, k, o, cb);
        };
    }

    function expect(f, os) {
        return function (_, cb) {
            var res = c.findObjects(b, f, {
                sort: {
                    attribute: 'num'
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

            // "within" tests
            expect('(num:within:=[-20,-10])', [ obj1, obj2 ]),
            expect('(num:within:=\\(-20,-10])', [ obj2 ]),
            expect('(num:within:=[-20.5,-19.5])', [ obj1 ]),
            expect('(num:within:=[-20,-10\\))', [ obj1 ]),
            expect('(num:within:=\\(0,\\))', [ obj3, obj4 ]),
            expect('(num:within:=[10,20])', [ obj3, obj4 ]),
            expect('(num:within:=[10,20\\))', [ obj3 ]),
            expect('(num:within:=\\(10,20])', [ obj4 ]),
            expect('(num:within:=[-15,15])', [ obj2, obj3 ]),
            expect('(num:within:=[,])', [ obj1, obj2, obj3, obj4 ]),
            expect('(num:within:=[,0])', [ obj1, obj2 ]),
            expect('(num:within:=[,0\\))', [ obj1, obj2 ]),
            expect('(num:within:=[0,])', [ obj3, obj4 ]),
            expect('(num:within:=\\(0,])', [ obj3, obj4 ]),
            expect('(num:within:=[-5,5])', []),
            expect('(num:within:=[15,15])', []),

            // "contains" tests
            expect('(numrange:contains:=-15)', [ obj1, obj3 ]),
            expect('(numrange:contains:=-10)', [ obj1, obj3 ]),
            expect('(numrange:contains:=-5)', [ obj1, obj3 ]),
            expect('(numrange:contains:=-4)', [ obj3 ]),
            expect('(numrange:contains:=0)', [ obj3 ]),
            expect('(numrange:contains:=4)', [ obj3 ]),
            expect('(numrange:contains:=5)', [ obj3, obj4 ]),
            expect('(numrange:contains:=15)', [ obj3, obj4 ]),
            expect('(numrange:contains:=20)', [ obj4 ]),

            // "overlaps" tests
            expect('(numrange:overlaps:=\\(-16,-4\\))', [ obj1, obj3 ]),
            expect('(numrange:overlaps:=\\(-14,-6\\))', [ obj1, obj3 ]),
            expect('(numrange:overlaps:=[-10,-10])', [ obj1, obj3 ]),
            expect('(numrange:overlaps:=[0,4])', [ obj3 ]),
            expect('(numrange:overlaps:=[0,10])', [ obj2, obj3, obj4 ]),
            expect('(numrange:overlaps:=[7,20])', [ obj2, obj3, obj4 ]),
            expect('(numrange:overlaps:=[15,20])', [ obj3, obj4 ]),
            expect('(numrange:overlaps:=\\(15,20])', [ obj4 ]),
            expect('(numrange:overlaps:=[20,])', [ obj4 ]),
            expect('(numrange:overlaps:=\\(,\\))', [ obj1, obj2, obj3, obj4 ]),
            expect('(numrange:overlaps:=[,])', [ obj1, obj2, obj3, obj4 ])
        ],
        arg: null
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});

test('Date ranges - findObjects()', function (t) {
    var key1 = uuid.v4();
    var obj1 = {
        date: '2016-06-05T16:23:04.776Z',
        daterange: '[1758-05-06T00:00:00.000Z,1993-05-06T00:00:00.000Z]'
    };

    var key2 = uuid.v4();
    var obj2 = {
        date: '2017-06-05T16:23:04.776Z',
        daterange: '(2018-01-01T12:00:00.000Z,2020-09-09T09:09:09.009Z)'
    };

    var key3 = uuid.v4();
    var obj3 = {
        date: '2018-03-09T12:00:00.000Z',
        daterange: '(,2020-09-09T09:09:09.009Z]'
    };

    var key4 = uuid.v4();
    var obj4 = {
        date: '2018-06-05T14:30:00.000Z',
        daterange: '[2005-05-05T05:05:05.555Z,)'
    };

    function put(k, o) {
        return function (_, cb) {
            c.putObject(b, k, o, cb);
        };
    }

    function expect(f, os) {
        return function (_, cb) {
            var res = c.findObjects(b, f, {
                sort: {
                    attribute: 'date'
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

            /* BEGIN JSSTYLED */

            // "within" tests
            expect('(date:within:=[2016-06-05T16:23:04.776Z,2017-06-05T16:23:04.776Z])', [ obj1, obj2 ]),
            expect('(date:within:=\\(2016-06-05T16:23:04.776Z,2017-06-05T16:23:04.776Z])', [ obj2 ]),
            expect('(date:within:=[2016-06-05T16:23:04.776Z,2017-06-05T16:23:04.776Z\\))', [ obj1 ]),
            expect('(date:within:=\\(2018-01-01T12:00:00Z,\\))', [ obj3, obj4 ]),
            expect('(date:within:=[2018-03-09T12:00:00Z,2018-06-05T14:30:00Z])', [ obj3, obj4 ]),
            expect('(date:within:=[2018-03-09T12:00:00Z,2018-06-05T14:30:00Z\\))', [ obj3 ]),
            expect('(date:within:=\\(2018-03-09T12:00:00Z,2018-06-05T14:30:00Z])', [ obj4 ]),
            expect('(date:within:=[2017-01-01T12:00:00Z,2018-05-05T12:00:00Z])', [ obj2, obj3 ]),
            expect('(date:within:=[,])', [ obj1, obj2, obj3, obj4 ]),
            expect('(date:within:=[,2018-01-01T12:00:00Z])', [ obj1, obj2 ]),
            expect('(date:within:=[,2018-01-01T12:00:00Z\\))', [ obj1, obj2 ]),
            expect('(date:within:=[2018-01-01T12:00:00Z,])', [ obj3, obj4 ]),
            expect('(date:within:=\\(2018-01-01T12:00:00Z,])', [ obj3, obj4 ]),

            // "contains" tests
            expect('(daterange:contains:=1758-05-06T00:00:00.000Z)', [ obj1, obj3 ]),
            expect('(daterange:contains:=1814-08-24T00:00:00.000Z)', [ obj1, obj3 ]),
            expect('(daterange:contains:=1945-05-08T00:00:00.000Z)', [ obj1, obj3 ]),
            expect('(daterange:contains:=1962-07-05T00:00:00.000Z)', [ obj1, obj3 ]),
            expect('(daterange:contains:=2005-04-27T00:00:00.000Z)', [ obj3 ]),
            expect('(daterange:contains:=2010-12-17T00:00:00.000Z)', [ obj3, obj4 ]),
            expect('(daterange:contains:=2999-12-31T23:59:59.999Z)', [ obj4 ]),

            // "overlaps" tests
            expect('(daterange:overlaps:=\\(1789-05-05T00:00:00.000Z,1799-11-09T00:00:00.000Z\\))', [ obj1, obj3 ]),
            expect('(daterange:overlaps:=[1993-05-06T00:00:00.000Z,1993-05-06T00:00:00.000Z])', [ obj1, obj3 ]),
            expect('(daterange:overlaps:=[2018-01-01T12:00:00.000Z,2999-12-31T23:59:59.999Z])', [ obj2, obj3, obj4 ]),
            expect('(daterange:overlaps:=[2020-09-09T09:09:09.009Z,2020-09-09T09:09:09.009Z])', [ obj3, obj4 ]),
            expect('(daterange:overlaps:=\\(2010-12-17T00:00:00.000Z,2013-06-06T00:00:00.000Z])', [ obj3, obj4 ]),
            expect('(daterange:overlaps:=\\(1739-10-22T00:00:00.000Z,1748-10-18T00:00:00.000Z\\))', [ obj3 ]),
            expect('(daterange:overlaps:=[2999-12-31T23:59:59.999Z,])', [ obj4 ]),
            expect('(daterange:overlaps:=\\(,\\))', [ obj1, obj2, obj3, obj4 ]),
            expect('(daterange:overlaps:=[,])', [ obj1, obj2, obj3, obj4 ])

            /* END JSSTYLED */
        ],
        arg: null
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});
