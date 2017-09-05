/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2016, Joyent, Inc.
 */

var tape = require('tape');
var util = require('util');
var uuid = require('libuuid').create;
var VError = require('verror');

var helper = require('./helper.js');


var c; // client
var server;
var table = 'moray_unit_test_' + uuid().substr(0, 7);
var sql;
var q;

function test(name, setup) {
    tape.test(name + ' - setup', function (t) {
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
        c.once('close', function () {
            helper.cleanupServer(server, function () {
                t.pass('closed');
                t.end();
            });
        });
        c.close();
    });
}


test('sql - execute', function (t) {
    sql = util.format('CREATE TABLE %s (value integer);', table);
    q = c.sql(sql, [], {});
    q.on('error', t.ifError.bind(t));
    q.once('end', function () {
        t.pass('create table success');
        t.end();
    });
});

test('sql - insert', function (t) {
    sql = util.format('INSERT INTO %s (value) VALUES ($1);', table);
    q = c.sql(sql, [5], {});
    q.on('error', t.ifError.bind(t));
    q.once('end', function () {
        t.pass('insert success');
        t.end();
    });
});

test('sql - select', function (t) {
    var count = 0;
    sql = util.format('SELECT * FROM %s', table);
    q = c.sql(sql, [], {});
    q.on('error', t.ifError.bind(t));
    q.on('record', function (row) {
        t.equal(row.value, 5);
        count++;
    });
    q.once('end', function () {
        t.equal(count, 1);
        t.end();
    });
});

test('sql - fail', function (t) {
    sql = 'BOGUS QUERY;';
    q = c.sql(sql, [], {});
    q.once('error', function (err) {
        t.ok(err, 'error returned');
        t.ok(VError.hasCauseWithName(err, 'InternalError'),
            'InternalError returned');
        t.end();
    });
});

test('sql - cleanup', function (t) {
    sql = util.format('DROP TABLE %s;', table);
    q = c.sql(sql, [], {});
    q.on('error', t.ifError.bind(t));
    q.once('end', function () {
        t.pass('success');
        t.end();
    });
});

test('sql - client timeout respected', function (t) {
    q = c.sql('SELECT pg_sleep(5);', [], { timeout: 1000 });
    q.on('error', function (err) {
        t.ok(VError.hasCauseWithName(err, 'QueryTimeoutError'),
            'correct error');
        t.end();
    });
    q.on('record', function (r) {
        t.fail('no row should be returned');
        t.deepEqual(r, null, 'no row should be returned - value');
    });
    q.on('end', function (r) {
        t.fail('"error" should have been emitted');
        t.end();
    });
});
