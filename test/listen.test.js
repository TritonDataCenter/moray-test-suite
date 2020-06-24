/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

/*
 * listen.test.js: ensure the listen() and unlisten() functionality works.
 */

var util = require('util');

var tape = require('tape');
var vasync = require('vasync');

var helper = require('./helper');

console.log('MORAY:', require.resolve('moray'));

tape.test('listen', function (t) {
    var ALL_NOTIFICATIONS_RECEIVED = 'all notifications received';
    var allNotificationsReceived = false;
    var channel = 'test-listen';
    var client1, client2;
    var clients = [];
    var listener;
    var notifications = [];
    var payloads = ['This is payload1', '{"data": "This is in json!"}'];

    vasync.pipeline({funcs: [
        function createClient1(_, callback) {
            client1 = helper.createClient();
            client1.once('connect', callback);
            client1.once('error', callback);
            clients.push(client1);
        },

        function createClient2(_, callback) {
            client2 = helper.createClient();
            client2.once('connect', callback);
            client2.once('error', callback);
            clients.push(client2);
        },

        function listenClient1(_, callback) {
            listener = client1.listen(channel);
            listener.on('readable', function _onListenReadable() {
                var notification = listener.read();
                while (notification) {
                    notifications.push(notification);
                    t.ok(notifications.length <= payloads.length,
                        util.format('Should get two notifications (%d/%d)',
                            notifications.length, payloads.length));
                    if (notifications.length == payloads.length) {
                        allNotificationsReceived = true;
                        client1.emit(ALL_NOTIFICATIONS_RECEIVED);
                    }
                    notification = listener.read();
                }
            });
            setImmediate(callback);
        },

        // Wait until the listener is fully setup. If we don't add this delay
        // then sometimes the test run will miss notifications (before the
        // listen call is listening). The reason for this is that the
        // morayClient.listen() call takes time to setup, but we do not receive
        // any notification for when it's ready - so we block and wait here,
        // and hopefully it will be ready after this short wait.
        function _wait(_, callback) {
            setTimeout(callback, 2000);
        },

        function notify(_, callback) {
            vasync.forEachPipeline({
                inputs: payloads,
                func: function _forEachPayload(payload, next) {
                    client2.notify(channel, payload, next);
                }
            }, callback);
        },

        function waitForNotifications(_, callback) {
            if (allNotificationsReceived) {
                setImmediate(callback);
                return;
            }
            client1.on(ALL_NOTIFICATIONS_RECEIVED, callback);
        },

        function verifyNotifications(_, callback) {
            t.equal(notifications.length, payloads.length,
                'Should have ' + payloads.length + ' notifications');
            notifications.forEach(function _eachNotification(notification, i) {
                t.equal(notification.channel, channel, 'Channel is equal');
                t.equal(notification.payload, payloads[i], 'Payload is equal');
            });
            setImmediate(callback);
        },

        function unlisten(_, callback) {
            listener.unlisten(callback);
        },

        function notifyAgain(_, callback) {
            vasync.forEachPipeline({
                inputs: payloads,
                func: function _forEachPayloadAgain(payload, next) {
                    client2.notify(channel, payload, next);
                }
            }, callback);
        },

        function verifyNoMoreNotifications(_, callback) {
            setTimeout(function _verify() {
                t.equal(notifications.length, payloads.length,
                    'Should still have ' + payloads.length + ' notifications');
                callback();
            }, 5000);
        }

    ]}, function (err) {
        t.ifError(err);
        clients.forEach(function _closeClient(c) {
            c.close();
        });
        t.end();
    });
});
