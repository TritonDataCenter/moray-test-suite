/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * This file implements a test suite for the "requireIndexes" option of
 * findObjects requests.
 */

var assert = require('assert-plus');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var tape = require('tape');
var vasync = require('vasync');
var VError = require('verror');

var helper = require('./helper.js');

/*
 * Since the crux of RFD 78 is to have findObjects requests error when the
 * search filter uses fields whose indexes are not usable yet, this test needs
 * to create a test bucket that has at least one index that is not usable.
 *
 * We do this by creating a new test bucket, and upgrading it to a second
 * version that adds a new index. This new index won't be usable until the
 * reindexObjects method is called and returns that the reindexing process has
 * completed.
 *
 * We need to have a test bucket that has more than one index because, even
 * without setting requireIndexes to true, findObjects requests fail with an
 * InvalidQueryError when a search filter contains _only one_ field whose index
 * is not usable.
 */
var BUCKET_CFG_V1 = {
    index: {
        foo: {
            type: 'string'
        }
    },
    options: {
        version: 1
    }
};

var BUCKET_CFG_V2 = {
    index: {
        foo: {
            type: 'string'
        },
        bar: {
            type: 'string'
        }
    },
    options: {
        version: 2
    }
};

var CLIENT_WITHOUT_REQUIRE_INDEXES;
var CLIENT_WITH_REQUIRE_INDEXES;

var SERVER;

var TEST_BUCKET = 'moray_unit_test_' + libuuid.create().substr(0, 7);

var TEST_OBJECT_KEY = 'someFoo';
var TEST_OBJECT_VALUE = {foo: 'bar', bar: 'baz'};
var TEST_SEARCH_FILTER = '(&(foo=bar)(bar=baz))';

/*
 * Some fields usable in a findObjects request's search filter have underlying
 * indexes that are usable from the time the bucket is created. We want to make
 * sure that setting requireIndexes to true doesn't change that.
 */
var FILTERS_ON_INTERNAL_FIELDS = [
    '(_mtime>=0)',
    '(_id=1)',
    '(_key=' + TEST_OBJECT_KEY + ')',
    '(&(_id=1)(_etag=*))'
];

/*
 * Reindexes all rows in the moray bucket with name "bucketName" using the moray
 * client "client". When all rows are reindexed, or if an error occurs, the
 * function "callback" is called. The first parameter of callback is an error
 * object or null if there was no error.
 */
function reindexBucket(bucketName, client, callback) {
    assert.string(bucketName, 'bucketName');
    assert.object(client, 'client');
    assert.func(callback, 'callback');

    function doReindex() {
        client.reindexObjects(bucketName, 100,
            function onBucketReindexed(reindexErr, result) {
                if (reindexErr || result.processed === 0) {
                    callback(reindexErr);
                    return;
                } else {
                    doReindex();
                    return;
                }
            });
    }

    doReindex();
}

tape.test('setup', function (t) {
    vasync.pipeline({arg: {}, funcs: [
        function createServer(ctx, next) {
            helper.createServer(null, function onServerCreated(server) {
                SERVER = server;
                next();
            });
        },
        function createClients(ctx, next) {
            CLIENT_WITH_REQUIRE_INDEXES = helper.createClient({
                requireIndexes: true
            });
            CLIENT_WITHOUT_REQUIRE_INDEXES = helper.createClient();
            CLIENT_WITHOUT_REQUIRE_INDEXES.on('connect',
                function onClientConnected() {
                    next();
                });
        },
        function createBucketV1(ctx, next) {
            CLIENT_WITHOUT_REQUIRE_INDEXES.createBucket(TEST_BUCKET,
                BUCKET_CFG_V1, function onBucketCreated(bucketCreateErr) {
                    t.ifErr(bucketCreateErr);
                    next();
                });
        },
        function putTestObject(ctx, next) {
            CLIENT_WITHOUT_REQUIRE_INDEXES.putObject(TEST_BUCKET,
                TEST_OBJECT_KEY, TEST_OBJECT_VALUE, {etag: null},
                function onPutObj(putObjErr, meta) {
                    t.ifErr(putObjErr);
                    t.ok(meta);
                    if (meta) {
                        t.ok(meta.etag);
                    }
                    next();
                });
        },
        function updateTestBucketToV2(ctx, next) {
            CLIENT_WITHOUT_REQUIRE_INDEXES.updateBucket(TEST_BUCKET,
                BUCKET_CFG_V2, function onBucketUpdate(bucketUpdateErr) {
                    t.ifErr(bucketUpdateErr);
                    next();
                });
        }
    ]}, function onTestSetupDone(testSetupErr) {
        t.end();
    });
});

tape.test('client() - findobjects()', function (t) {
    helper.performFindObjectsTest(t, CLIENT_WITHOUT_REQUIRE_INDEXES, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER,
        findObjectsOpts: {},
        expectedResults: {
            /*
             * Not using requireIndexes: true should not result in an error,
             * even if the test filter contains fields whose index is not
             * usable.
             */
            error: false,
            /*
             * Because the field is being reindexed, Moray will drop it from
             * the WHERE clause, and check the value of the field after fetching
             * it.
             */
            nbRecordsFound: 1
        }
    });
});

tape.test('client() - findobjects({requireIndexes: false})', function (t) {
    helper.performFindObjectsTest(t, CLIENT_WITHOUT_REQUIRE_INDEXES, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER,
        findObjectsOpts: {requireIndexes: false},
        expectedResults: {
            error: false,
            nbRecordsFound: 1
        }
    });
});

tape.test('client() - findobjects({requireIndexes: true})', function (t) {
    helper.performFindObjectsTest(t, CLIENT_WITHOUT_REQUIRE_INDEXES, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER,
        findObjectsOpts: {requireIndexes: true},
        expectedResults: {
            /*
             * Using requireIndexes: true should result in an error, because the
             * test filter contains fields whose index is not usable.
             */
            error: true,
            nbRecordsFound: 0,
            errMsg: TEST_BUCKET + ' does not have indexes that support ' +
                TEST_SEARCH_FILTER + '. Reindexing fields: [ \'bar\' ]. ' +
                'Unindexed fields: []'
        }
    });
});

function createTestFindobjectsRequireIndexes(searchFilter) {
    tape.test('client() - findobjects(' + searchFilter + ', {requireIndexes: ' +
        'true})', function (t) {
        helper.performFindObjectsTest(t, CLIENT_WITHOUT_REQUIRE_INDEXES, {
            bucketName: TEST_BUCKET,
            searchFilter: searchFilter,
            findObjectsOpts: {requireIndexes: true},
            expectedResults: {
                /*
                 * All the internal fields are usable in a search filter from
                 * the time the bucket is created. So even when passing
                 * requireIndexes: true to findObjects, the request should not
                 * error.
                 */
                error: false,
                nbRecordsFound: 1
            }
        });
    });
}

FILTERS_ON_INTERNAL_FIELDS.forEach(createTestFindobjectsRequireIndexes);

tape.test('client({requireIndexes: true}) - findobjects()', function (t) {
    helper.performFindObjectsTest(t, CLIENT_WITH_REQUIRE_INDEXES, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER,
        findObjectsOpts: {},
        expectedResults: {
            /*
             * Using requireIndexes: true when instantiating the moray client
             * should make findObjects requests result in an error, because the
             * test filter contains fields whose index is not usable.
             */
            error: true,
            nbRecordsFound: 0,
            errMsg: TEST_BUCKET + ' does not have indexes that support ' +
                TEST_SEARCH_FILTER + '. Reindexing fields: [ \'bar\' ]. ' +
                'Unindexed fields: []'
        }
    });
});

FILTERS_ON_INTERNAL_FIELDS.forEach(function (searchFilter) {
    tape.test('client({requireIndexes: true}) - findobjects(' + searchFilter +
        ')', function (t) {
        helper.performFindObjectsTest(t, CLIENT_WITH_REQUIRE_INDEXES, {
            bucketName: TEST_BUCKET,
            searchFilter: searchFilter,
            findObjectsOpts: {},
            expectedResults: {
                /*
                 * All the internal fields are usable in a search filter from
                 * the time the bucket is created. So even when passing
                 * requireIndexes: true when instantiating the moray client,
                 * findObjects requests should not error.
                 */
                error: false,
                nbRecordsFound: 1
            }
        });
    });
});

tape.test('client({requireIndexes: true}) - findobjects({requireIndexes: ' +
    'true})', function (t) {
    helper.performFindObjectsTest(t, CLIENT_WITH_REQUIRE_INDEXES, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER,
        findObjectsOpts: {requireIndexes: true},
        expectedResults: {
            /*
             * Using requireIndexes: true when instantiating the moray client
             * _and_ passing requireIndexes: true to findObjects should make
             * findObjects requests result in an error, because the test filter
             * contains fields whose index is not usable.
             */
            error: true,
            nbRecordsFound: 0,
            errMsg: TEST_BUCKET + ' does not have indexes that support ' +
                TEST_SEARCH_FILTER + '. Reindexing fields: [ \'bar\' ]. ' +
                'Unindexed fields: []'
        }
    });
});

function createTestClientRequiresIndexes(searchFilter) {
    tape.test('client({requireIndexes: true}) - findobjects(' + searchFilter +
        ', {requireIndexes: true})', function (t) {
        helper.performFindObjectsTest(t, CLIENT_WITH_REQUIRE_INDEXES, {
            bucketName: TEST_BUCKET,
            searchFilter: searchFilter,
            findObjectsOpts: {requireIndexes: true},
            expectedResults: {
                /*
                 * All the internal fields are usable in a search filter from
                 * the time the bucket is created. So even when passing
                 * requireIndexes: true when instantiating the moray client
                 * _and_ passing requireIndexes: true to the findObjects method,
                 * findObjects requests should not error.
                 */
                error: false,
                nbRecordsFound: 1
            }
        });
    });
}

FILTERS_ON_INTERNAL_FIELDS.forEach(createTestClientRequiresIndexes);

tape.test('client({requireIndexes: true}) - findobjects({requireIndexes: ' +
    'false})', function (t) {
    helper.performFindObjectsTest(t, CLIENT_WITH_REQUIRE_INDEXES, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER,
        findObjectsOpts: {requireIndexes: false},
        expectedResults: {
            /*
             * Using requireIndexes: true when instantiating the moray client
             * _but_ passing requireIndexes: false to findObjects should make
             * findObjects requests _not_ result in an error.
             */
            error: false,
            nbRecordsFound: 1
        }
    });
});

function createTestRequireIndexesOverriden(searchFilter) {
    tape.test('client({requireIndexes: true}) - findobjects(' + searchFilter +
        ', {requireIndexes: false})', function (t) {
        helper.performFindObjectsTest(t, CLIENT_WITH_REQUIRE_INDEXES, {
            bucketName: TEST_BUCKET,
            searchFilter: searchFilter,
            findObjectsOpts: {requireIndexes: false},
            expectedResults: {
                /*
                 * Using requireIndexes: true when instantiating the moray
                 * client _but_ passing requireIndexes: false to findObjects
                 * should make findObjects requests _not_ result in an error.
                 */
                error: false,
                nbRecordsFound: 1
            }
        });
    });
}

FILTERS_ON_INTERNAL_FIELDS.forEach(createTestRequireIndexesOverriden);

/*
 * After this test completes, the second index that was added when upgrading the
 * bucket "TEST_BUCKET" to its second version is usable, so no findObjects
 * request using any combination of the two indexed fields as a search filter
 * should error, regardless of what value for requireIndexes is passed when
 * instantiating a moray client or when calling its findObjects method.
 */
tape.test('reindexObjects', function (t) {
    reindexBucket(TEST_BUCKET, CLIENT_WITHOUT_REQUIRE_INDEXES,
        function onReindexDone(reindexErr) {
            t.ifErr(reindexErr);
            t.end();
        });
});

tape.test('client() - findobjects()', function (t) {
    helper.performFindObjectsTest(t, CLIENT_WITHOUT_REQUIRE_INDEXES, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER,
        findObjectsOpts: {},
        expectedResults: {
            error: false,
            nbRecordsFound: 1
        }
    });
});

tape.test('client() - findobjects({requireIndexes: true})', function (t) {
    helper.performFindObjectsTest(t, CLIENT_WITHOUT_REQUIRE_INDEXES, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER,
        findObjectsOpts: {requireIndexes: true},
        expectedResults: {
            error: false,
            nbRecordsFound: 1
        }
    });
});

tape.test('client({requireIndexes: true}) - findobjects()', function (t) {
    helper.performFindObjectsTest(t, CLIENT_WITH_REQUIRE_INDEXES, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER,
        findObjectsOpts: {},
        expectedResults: {
            error: false,
            nbRecordsFound: 1
        }
    });
});

tape.test('client({requireIndexes: true}) - findobjects({requireIndexes: ' +
    'false})', function (t) {
    helper.performFindObjectsTest(t, CLIENT_WITH_REQUIRE_INDEXES, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER,
        findObjectsOpts: {requireIndexes: false},
        expectedResults: {
            error: false,
            nbRecordsFound: 1
        }
    });
});

tape.test('client({requireIndexes: true}) - findobjects({requireIndexes: ' +
    'true})', function (t) {
    helper.performFindObjectsTest(t, CLIENT_WITH_REQUIRE_INDEXES, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER,
        findObjectsOpts: {requireIndexes: true},
        expectedResults: {
            error: false,
            nbRecordsFound: 1
        }
    });
});

tape.test('teardown', function (t) {
    function closeServerAndEnd() {
        helper.cleanupServer(SERVER, function onCleanupServer() {
            t.pass('closed');
            t.end();
        });
    }

    function onClientClosed() {
        ++nbClientsClosed;
        if (nbClientsClosed === 2) {
            closeServerAndEnd();
        }
    }

    var nbClientsClosed = 0;

    CLIENT_WITHOUT_REQUIRE_INDEXES.delBucket(TEST_BUCKET,
        function onDelBucket(delBucketErr) {
            t.ifErr(delBucketErr);

            CLIENT_WITHOUT_REQUIRE_INDEXES.once('close', onClientClosed);
            CLIENT_WITHOUT_REQUIRE_INDEXES.once('close', onClientClosed);

            CLIENT_WITHOUT_REQUIRE_INDEXES.close();
            CLIENT_WITH_REQUIRE_INDEXES.close();
        });
});
