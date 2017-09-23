/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */
var libuuid = require('libuuid');
var tape = require('tape');
var vasync = require('vasync');
var jsprim = require('jsprim');
var helper = require('./helper');

/*
 * This file implements a test suite for a number of findObjects operations
 * including various options such as offset, limit, sorting, etc.
 */
var CLIENT;
var SERVER;

var TEST_BUCKET_CFG = {
    index: {
        foo: {
            type: 'string'
        },
        bar: {
            type: 'string'
        },
        fooArray: {
            type: '[string]'
        },
        addr: {
            type: 'ip'
        },
        net: {
            type: 'subnet'
        },
        sort_by: {
            type: 'number'
        },
        sort_by_other: {
            type: 'number'
        },
        sort_by_one: {
            type: 'number'
        },
        sort_by_two: {
            type: 'number'
        },
        singleton: {
            type: 'boolean'
        }
    },
    options: {
        version: 1
    }
};

var TEST_LARGE_BUCKET_CFG = {
    index: {
        foo: {
            type: 'number'
        }
    }
};

var TEST_BUCKET = 'moray_findobjects_unit_test' + libuuid.create().substr(0, 7);
var TEST_LARGE_BUCKET = 'moray_findobjects_unit_test_large' +
    libuuid.create().substr(0, 7);

/*
 * Filters used to test various options and other findobjects behaviors.
 */
var TEST_SEARCH_FILTER_BAD_AND = '(&(nonexisting=field)(notthere=filter))';
var TEST_SEARCH_FILTER_BAD_OR = '(|(nonexisting=field))';

var TEST_SEARCH_FILTER_BAD_SUBSTR_INITIAL = '(fooArray=initial*)';
var TEST_SEARCH_FILTER_BAD_SUBSTR_ANY = '(fooArray=*any*)';
var TEST_SEARCH_FILTER_BAD_SUBSTR_FINAL = '(fooArray=*final)';
var TEST_SEARCH_FILTER_BAD_EXT = '(foo:fakeExt:=bad)';

var TEST_IP_GE_FILTER = '(addr>=192.168.1.0)';
var TEST_IP_LE_FILTER = '(addr<=192.168.1.0)';

var TEST_SUBNET_GE_FILTER = '(net>=192.168.1.0/24)';
var TEST_SUBNET_LE_FILTER = '(net<=192.168.1.0/24)';

var TEST_SORT_SINGLETON_FILTER = '(singleton=true)';

// Used in the noLimit test. This value is 1 greater than the default
// limit on number of rows returned per query by moray.
const NUM_OBJECTS_NOLIMIT = 1001;

/*
 * These test objects are added to the test bucket on setup. They have various
 * uncorrelated fields that are referenced by some of the above filters.
 */
var TEST_OBJECTS = {
    'obj1': {
        sort_by: 1,
        sort_by_other: 3,
        sort_by_one: 1,
        sort_by_two: 2,
        singleton: true,
        net: '192.168.1.0/24',
        addr: '192.168.1.0'
    },
    'obj2': {
        sort_by: 2,
        sort_by_other: 2,
        sort_by_one: 2,
        sort_by_two: 2,
        singleton: false,
        net: '192.168.0.0/24',
        addr: '192.168.0.255'
    },
    'obj3': {
        sort_by: 3,
        sort_by_other: 1,
        sort_by_one: 3,
        sort_by_two: 3,
        singleton: false,
        net: '192.168.2.0/24',
        addr: '192.168.1.1'
    }
};

var NUM_TEST_OBJECTS = Object.keys(TEST_OBJECTS).length;

tape.test('setup', function (t) {
    vasync.pipeline({arg: {}, funcs: [
        function createServer(ctx, next) {
            helper.createServer(null, function onServerCreated(server) {
                SERVER = server;
                next();
            });
        },
        function createClient(ctx, next) {
            CLIENT = helper.createClient();
            CLIENT.on('connect', function onClientConnected() {
                next();
            });
        },
        function createBucket(ctx, next) {
            CLIENT.createBucket(TEST_BUCKET, TEST_BUCKET_CFG,
                function onBucketCreated(bucketCreateErr) {
                    t.ifErr(bucketCreateErr);
                    next();
                });
        },
        function putTestObjects(ctx, next) {

            function putObject(key, cb) {
                CLIENT.putObject(TEST_BUCKET, key, TEST_OBJECTS[key],
                        {etag: null}, function onPutObj(putObjErr, meta) {
                            t.ifErr(putObjErr);
                            if (putObjErr) {
                                cb(putObjErr);
                                return;
                            }
                            t.ok(meta);
                            if (meta) {
                                t.ok(meta.etag);
                            }
                            cb(null, meta);
                        });
            }

            vasync.forEachPipeline({
                func: putObject,
                inputs: Object.keys(TEST_OBJECTS)
            }, function (err, results) {
                next();
            });
        },
        function createLargeBucket(ctx, next) {
             CLIENT.createBucket(TEST_LARGE_BUCKET, TEST_LARGE_BUCKET_CFG,
                function onBucketCreated(bucketCreateErr) {
                    t.ifErr(bucketCreateErr);
                    next();
                });
        },
        function putObjectsLargeBucket(ctx, next) {
            var requests = [];
            for (var i = 0; i < NUM_OBJECTS_NOLIMIT; i++) {
                requests.push({
                    bucket: TEST_LARGE_BUCKET,
                    operation: 'put',
                    key: libuuid.create(),
                    value: {
                        foo: i
                    }
                });
            }
            CLIENT.batch(requests, function (err, meta) {
                t.ifError(err);
                next();
            });
        }
    ]}, function onTestSetupDone(testSetupErr) {
        t.end();
    });
});

tape.test('findobjects - bad \'&\' filter', function (t) {
    helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER_BAD_AND,
        findObjectsOpts: {},
        expectedResults: {
            error: true,
            nbRecordsFound: 0,
            /*
             * This query results in an error because neither of the
             * fields in the search filter exists and are therefore not
             * indexed.
             */
            errMsg: TEST_BUCKET + ' does not have indexes that support ' +
                TEST_SEARCH_FILTER_BAD_AND
        }
    });
});

tape.test('findobjects - bad \'|\' filter', function (t) {
    helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER_BAD_OR,
        findObjectsOpts: {},
        expectedResults: {
            /*
             * Test filter contains fields whose index is not usable.
             */
            error: true,
            nbRecordsFound: 0,
            errMsg: TEST_BUCKET + ' does not have indexes that support ' +
                TEST_SEARCH_FILTER_BAD_OR
        }
    });
});

tape.test('findobjects - prefix search on array field', function (t) {
    helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER_BAD_SUBSTR_INITIAL,
        findObjectsOpts: {requireIndexes: false},
        expectedResults: {
            error: true,
            nbRecordsFound: 0,
            errMsg: TEST_BUCKET + ' does not have indexes that support ' +
                TEST_SEARCH_FILTER_BAD_SUBSTR_INITIAL
        }
    });
});

tape.test('findobjects - inner search on array field', function (t) {
    helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER_BAD_SUBSTR_ANY,
        findObjectsOpts: {requireIndexes: false},
        expectedResults: {
            error: true,
            nbRecordsFound: 0,
            errMsg: TEST_BUCKET + ' does not have indexes that support ' +
                TEST_SEARCH_FILTER_BAD_SUBSTR_ANY
        }
    });
});

tape.test('findobjects - suffix search on array field', function (t) {
    helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SEARCH_FILTER_BAD_SUBSTR_ANY,
        findObjectsOpts: {requireIndexes: false},
        expectedResults: {
            error: true,
            nbRecordsFound: 0,
            errMsg: TEST_BUCKET + ' does not have indexes that support ' +
                TEST_SEARCH_FILTER_BAD_SUBSTR_ANY
        }
    });
});

tape.test('findobjects - >= filter on \'ip\' type', function (t) {
     helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_IP_GE_FILTER,
        findObjectsOpts: {requireIndexes: false},
        expectedResults: {
            error: false,
            nbRecordsFound: 2,
            verifyRecords: function (records) {
                var values = records.map(function (r) { return r.value; });
                return jsprim.deepEqual(values, [
                    TEST_OBJECTS.obj1,
                    TEST_OBJECTS.obj3
                ]);
            }
        }
    });
});

tape.test('findobjects - <= filter on \'ip\' type', function (t) {
     helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_IP_LE_FILTER,
        findObjectsOpts: {requireIndexes: false},
        expectedResults: {
            error: false,
            nbRecordsFound: 2,
            verifyRecords: function (records) {
                var values = records.map(function (r) { return r.value; });
                return jsprim.deepEqual(values, [
                    TEST_OBJECTS.obj1,
                    TEST_OBJECTS.obj2
                ]);
            }
        }
    });
});

tape.test('findobjects - >= filter on \'subnet\' type', function (t) {
     helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SUBNET_GE_FILTER,
        findObjectsOpts: {requireIndexes: false},
        expectedResults: {
            error: false,
            nbRecordsFound: 2,
            verifyRecords: function (records) {
                var values = records.map(function (r) { return r.value; });
                return jsprim.deepEqual(values, [
                    TEST_OBJECTS.obj1,
                    TEST_OBJECTS.obj3
                ]);
            }
        }
    });
});

tape.test('findobjects - <= filter on \'subnet\' type', function (t) {
     helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: TEST_SUBNET_LE_FILTER,
        findObjectsOpts: {requireIndexes: false},
        expectedResults: {
            error: false,
            nbRecordsFound: 2,
            verifyRecords: function (records) {
                var values = records.map(function (r) { return r.value; });
                return jsprim.deepEqual(values, [
                    TEST_OBJECTS.obj1,
                    TEST_OBJECTS.obj2
                ]);
            }
        }
    });
});

tape.test('findobjects - ascending sort on multiple objects', function (t) {
     helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: '(sort_by=*)',
        findObjectsOpts: {
            sort: {
                order: 'ASC',
                attribute: 'sort_by'
            }
        },
        expectedResults: {
            error: false,
            nbRecordsFound: NUM_TEST_OBJECTS,
            verifyRecords: function (records) {
                var values = records.map(function (r) { return r.value; });
                return jsprim.deepEqual(values, [
                    TEST_OBJECTS.obj1,
                    TEST_OBJECTS.obj2,
                    TEST_OBJECTS.obj3
                ]);
            }
        }
    });
});

tape.test('findobjects - descending sort on multiple objects', function (t) {
     helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: '(sort_by=*)',
        findObjectsOpts: {
            sort: {
                order: 'DESC',
                attribute: 'sort_by'
            }
        },
        expectedResults: {
            error: false,
            nbRecordsFound: NUM_TEST_OBJECTS,
            verifyRecords: function (records) {
                var values = records.map(function (r) { return r.value; });
                return jsprim.deepEqual(values, [
                    TEST_OBJECTS.obj3,
                    TEST_OBJECTS.obj2,
                    TEST_OBJECTS.obj1
                ]);
            }
        }
    });
});

tape.test('findobjects - ascending sort on single object', function (t) {
     helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: '(&(sort_by=*)(singleton=true))',
        findObjectsOpts: {
            sort: {
                order: 'ASC',
                attribute: 'sort_by'
            }
        },
        expectedResults: {
            error: false,
            nbRecordsFound: 1,
            verifyRecords: function (records) {
                return jsprim.deepEqual(records[0].value, TEST_OBJECTS.obj1);
            }
        }
    });
});

tape.test('findobjects - descending sort on single object', function (t) {
     helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: '(&(sort_by=*)(singleton=true))',
        findObjectsOpts: {
            sort: {
                order: 'DESC',
                attribute: 'sort_by'
            }
        },
        expectedResults: {
            error: false,
            nbRecordsFound: 1,
            verifyRecords: function (records) {
                return jsprim.deepEqual(records[0].value, TEST_OBJECTS.obj1);
            }
        }
    });
});

tape.test('findobjects - combined ascending/descending sort', function (t) {
     helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: '(sort_by=*)',
        findObjectsOpts: {
            sort: [
                {
                    order: 'DESC',
                    attribute: 'sort_by'
                },
                {
                    order: 'ASC',
                    attribute: 'sort_by_other'
                }
            ]
        },
        expectedResults: {
            error: false,
            nbRecordsFound: 3,
            verifyRecords: function (records) {
                var values = records.map(function (r) { return r.value; });
                return jsprim.deepEqual(values, [
                    TEST_OBJECTS.obj3,
                    TEST_OBJECTS.obj2,
                    TEST_OBJECTS.obj1
                ]);
            }
        }
    });
});

/*
 * Conceptually, the records in this test can be thought as tuples:
 * (1, 2), (2, 2), (3, 3). The second number in each tuple represents
 * sort_by_two, and the first represents sort_by_one. The test verifies
 * that if we sort by the second number first, and then sort by the
 * first number - that we get the following ordering:
 *
 *              (1, 2), (2, 2), (3, 3)
 *
 * In particular, we do not want to see: (2, 2), (1, 2), (3, 3), which
 * is a possible ordering that could be returned upon sorting by only
 * the second field.
 */
tape.test('findobjects - multi-constraint sort tiebreaking', function (t) {
     helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: '(sort_by=*)',
        findObjectsOpts: {
            sort: [
                {
                    order: 'ASC',
                    attribute: 'sort_by_two'
                },
                {
                    order: 'ASC',
                    attribute: 'sort_by_one'
                }
            ]
        },
        expectedResults: {
            error: false,
            nbRecordsFound: 3,
            verifyRecords: function (records) {
                var values = records.map(function (r) { return r.value; });
                return jsprim.deepEqual(values, [
                    TEST_OBJECTS.obj1,
                    TEST_OBJECTS.obj2,
                    TEST_OBJECTS.obj3
                ]);
            }
        }
    });
});

tape.test('findobjects - noLimit returns full set', function (t) {
     helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_LARGE_BUCKET,
        searchFilter: '(foo=*)',
        findObjectsOpts: {
            noLimit: true
        },
        expectedResults: {
            error: false,
            nbRecordsFound: NUM_OBJECTS_NOLIMIT
        }
    });
});

tape.test('findobjects - limit option as number', function (t) {
     helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: '(sort_by=*)',
        findObjectsOpts: {
            limit: 1
        },
        expectedResults: {
            error: false,
            nbRecordsFound: 1
        }
    });
});

tape.test('findobjects - limit option as string', function (t) {
    var limit = '1';
    helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: '(sort_by=*)',
        findObjectsOpts: {
            limit: limit
        },
        expectedResults: {
            error: false,
            nbRecordsFound: 1
        }
    });
});

tape.test('findobjects - offset option as number', function (t) {
    var offset = 2;
    helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: '(sort_by=*)',
        findObjectsOpts: {
            offset: offset
        },
        expectedResults: {
            error: false,
            nbRecordsFound: (NUM_TEST_OBJECTS - offset)
        }
    });
});

tape.test('findobjects - offset option as string', function (t) {
    var offset = '2';
    var numeric = 2;
    helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: '(sort_by=*)',
        findObjectsOpts: {
            offset: offset
        },
        expectedResults: {
            error: false,
            nbRecordsFound: (NUM_TEST_OBJECTS - numeric)
        }
    });
});

tape.test('findobjects - sql_only', function (t) {
     helper.performFindObjectsTest(t, CLIENT, {
        bucketName: TEST_BUCKET,
        searchFilter: '(sort_by=*)',
        findObjectsOpts: {
            sql_only: true
        },
        expectedResults: {
            error: false,
            nbRecordsFound: 1,
            verifyRecords: function (records) {
                for (var i = 0; i < records.length; i++) {
                    if (!records[i].query || !records[i].args) {
                        return false;
                    }
                }
                return true;
            }
        }
    });
});

tape.test('teardown', function (t) {
    vasync.waterfall([
        function (callback) {
            CLIENT.delBucket(TEST_BUCKET, function onDelBucket(delBucketErr) {
                t.ifErr(delBucketErr);
                callback();
            });
        },
        function (callback) {
            CLIENT.delBucket(TEST_LARGE_BUCKET,
                function onDelBucket(delBucketErr) {
                    t.ifErr(delBucketErr);
                    callback();
                });
        }
    ], function () {
        CLIENT.once('close', function closeServerAndEnd() {
            helper.cleanupServer(SERVER, function onCleanupServer() {
                t.pass('closed');
                t.end();
            });
        });
        CLIENT.close();
    });
});
