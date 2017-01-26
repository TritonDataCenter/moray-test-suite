/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * clientparams.test.js: tests parsing various combinations of client parameters
 */

var assertplus = require('assert-plus');
var moray = require('moray');
var tape = require('tape');
var parseMorayParameters = moray.Client.privateParseMorayParameters;

var helper = require('./helper');

var testcases;

function main()
{
    testcases.forEach(function (tc) {
        helper.defineStatelessTestCase(tape, parseMorayParameters, tc);
    });
}

testcases = [

/* Invalid arguments */
{
    'name': 'error case: missing arguments',
    'input': {},
    'errmsg': /at least one of .* must be specified/
}, {
    'name': 'error case: url: bad type',
    'input': {
        'url': {}
    },
    'errmsg': /args.url \(string\) is required$/
}, {
    'name': 'error case: host: bad type',
    'input': {
        'host': {}
    },
    'errmsg': /args.host \(string\) is required$/
}, {
    'name': 'error case: port: bad type',
    'input': {
        'host': 'dental.plan',
        'port': {}
    },
    'errmsg': /args.port must be a string or number/
}, {
    'name': 'error case: srvDomain with an IP address',
    'input': {
        'srvDomain': '127.0.0.1'
    },
    /* JSSTYLED */
    'errmsg': /cannot use "srvDomain" with an IP address/
}, {
    'name': 'error case: cueballOptions: bad type',
    'input': {
        'cueballOptions': 47
    },
    'errmsg': /args.cueballOptions \(object\) is required$/
}, {
    'name': 'error case: cueballOptions.target: requires maximum',
    'input': {
        'cueballOptions': {
            'target': 12
        }
    },
    /* JSSTYLED */
    'errmsg': /must specify neither or both of "target" and "maximum"/
}, {
    'name': 'error case: cueballOptions.maximum: requires target',
    'input': {
        'cueballOptions': {
            'maximum': 12
        }
    },
    /* JSSTYLED */
    'errmsg': /must specify neither or both of "target" and "maximum"/
}, {
    'name': 'error case: cueballOptions.target: bad type',
    'input': {
        'cueballOptions': {
            'target': 'foo'
        }
    },
    'errmsg': /args.cueballOptions.target \(number\) is required$/
}, {
    'name': 'error case: cueballOptions.maximum: bad type',
    'input': {
        'cueballOptions': {
            'maximum': 'foo'
        }
    },
    'errmsg': /args.cueballOptions.maximum \(number\) is required$/
}, {
    'name': 'error case: cueballOptions.maxDNSConcurrency: bad type',
    'input': {
        'cueballOptions': {
            'maxDNSConcurrency': 'foo'
        }
    },
    'errmsg': /args.cueballOptions.maxDNSConcurrency \(number\) is required$/
}, {
    'name': 'error case: cueballOptions.recovery: bad type',
    'input': {
        'cueballOptions': {
            'recovery': 38
        }
    },
    'errmsg': /args.cueballOptions.recovery \(object\) is required$/
}, {
    'name': 'error case: cueballOptions.resolvers: bad type (number)',
    'input': {
        'cueballOptions': {
            'resolvers': 38
        }
    },
    'errmsg': /args.cueballOptions.resolvers \(\[string\]\) is required$/
}, {
    'name': 'error case: cueballOptions.resolvers: bad type (object)',
    'input': {
        'cueballOptions': {
            'resolvers': {}
        }
    },
    'errmsg': /args.cueballOptions.resolvers \(\[string\]\) is required$/
}, {
    'name': 'error case: cueballOptions.defaultPort: bad type',
    'input': {
        'cueballOptions': {
            'defaultPort': 'braces'
        }
    },
    'errmsg': /args.cueballOptions.defaultPort \(number\) is required$/
}, {
    'name': 'error case: cueballOptions.resolvers: bad element types',
    'input': {
        'cueballOptions': {
            'resolvers': [ 37 ]
        }
    },
    'errmsg': /args.cueballOptions.resolvers \(\[string\]\) is required$/
}, {
    'name': 'error case: cueballOptions.service: bad type',
    'input': {
        'host': 'dental.plan',
        'cueballOptions': {
            'service': 38
        }
    },
    'errmsg': /args.cueballOptions.service \(string\) is required$/
}, {
    'name': 'error case: cueballOptions.domain: not allowed',
    'input': {
        'host': 'dental.plan',
        'cueballOptions': {
            'domain': 'foobar'
        }
    },
    /* JSSTYLED */
    'errmsg': /"domain" may not be specified in cueballOptions$/
}, {
    'name': 'error case: connectTimeout: bad type',
    'input': {
        'host': 'dental.plan',
        'connectTimeout': '1234'
    },
    'errmsg': /args.connectTimeout \(number\) is required$/
}, {
    'name': 'error case: maxConnections: bad type',
    'input': {
        'host': 'dental.plan',
        'maxConnections': {}
    },
    'errmsg': /args.maxConnections \(number\) is required$/
}, {
    'name': 'error case: dns: bad type',
    'input': {
        'host': 'dental.plan',
        'dns': 17
    },
    'errmsg': /args.dns \(object\) is required$/
}, {
    'name': 'error case: retry: bad type',
    'input': {
        'host': 'dental.plan',
        'retry': 37
    },
    'errmsg': /args.retry \(object\) is required$/
}, {
    'name': 'error case: retry: bad type',
    'input': {
        'host': 'dental.plan',
        'retry': 37
    },
    'errmsg': /args.retry \(object\) is required$/
}, {
    'name': 'error case: retry: minTimeout > maxTimeout',
    'input': {
        'host': 'dental.plan',
        'retry': {
            'retries': 42,
            'minTimeout': 7890,
            'maxTimeout': 4567
        }
    },
    'errmsg': /maxTimeout.*minTimeout/
},

/* Valid arguments */

{
    /*
     * This is one of the most important default configurations.  This is also
     * one of the only ones for which we bother verifying all the default
     * filled-in values.
     */
    'name': 'srvDomain specified',
    'input': { 'srvDomain': 'chilitown' },
    'output': {
        'mode': 'srv',
        'label': 'chilitown',
        'cueballOptions': {
            'domain': 'chilitown',
            'service': '_moray._tcp',
            'defaultPort': 2020,
            'maxDNSConcurrency': 3,
            'target': 6,
            'maximum': 15,
            'recovery': {
                'default': {
                    'retries': 5,
                    'timeout': 2000,
                    'maxTimeout': 30000,
                    'delay': 1000,
                    'maxDelay': 60000
                },
                'dns': {
                    'retries': 5,
                    'timeout': 1000,
                    'maxTimeout': 20000,
                    'delay': 10,
                    'maxDelay': 10000
                },
                'dns_srv': {
                    'retries': 0,
                    'timeout': 1000,
                    'maxTimeout': 20000,
                    'delay': 10,
                    'maxDelay': 10000
                }
            }
        }
    }
}, {
    /* This is one of the most important default configurations. */
    'name': 'srvDomain specified with failFast',
    'input': { 'srvDomain': 'chilitown', 'failFast': true },
    'output': {
        'mode': 'srv',
        'label': 'chilitown',
        'cueballOptions': {
            'domain': 'chilitown',
            'service': '_moray._tcp',
            'defaultPort': 2020,
            'maxDNSConcurrency': 3,
            'target': 6,
            'maximum': 15,
            'recovery': {
                'default': {
                    'retries': 0,
                    'timeout': 2000,
                    'maxTimeout': 30000,
                    'delay': 0,
                    'maxDelay': 0
                },
                'dns': {
                    'retries': 5,
                    'timeout': 1000,
                    'maxTimeout': 20000,
                    'delay': 10,
                    'maxDelay': 10000
                },
                'dns_srv': {
                    'retries': 0,
                    'timeout': 1000,
                    'maxTimeout': 20000,
                    'delay': 10,
                    'maxDelay': 10000
                }
            }
        }
    }
}, {
    /* This is one of the most important default configurations. */
    'name': 'host specified, missing port',
    'input': { 'host': 'foobar' },
    'output': {
        'mode': 'direct',
        'label': 'foobar:2020',
        'cueballOptions': {
            'domain': 'foobar',
            'service': '_moraybogus._tcp',
            'defaultPort': 2020
        }
    }
}, {
    /* This is one of the most important default configurations. */
    'name': 'host specified with port',
    'input': { 'host': 'foobar', 'port': 2021 },
    'output': {
        'mode': 'direct',
        'label': 'foobar:2021',
        'cueballOptions': {
            'domain': 'foobar',
            'service': '_moraybogus._tcp',
            'defaultPort': 2021
        }
    }
}, {
    'name': 'url specified with IP and port',
    'input': { 'url': 'tcp://10.1.2.3:2022' },
    'output': {
        'mode': 'direct',
        'label': '10.1.2.3:2022',
        'cueballOptions': {
            'domain': '10.1.2.3',
            'service': '_moraybogus._tcp',
            'defaultPort': 2022
        }
    }
}, {
    'name': 'url specified with hostname and port',
    'input': { 'url': 'tcp://chilitown:2023/' },
    'output': {
        'mode': 'direct',
        'label': 'chilitown:2023',
        'cueballOptions': {
            'domain': 'chilitown',
            'service': '_moraybogus._tcp',
            'defaultPort': 2023
        }
    }
}, {
    'name': 'url specified with hostname only',
    'input': { 'url': 'tcp://chilitown/' },
    'output': {
        'mode': 'direct',
        'label': 'chilitown:2020',
        'cueballOptions': {
            'domain': 'chilitown',
            'service': '_moraybogus._tcp',
            'defaultPort': 2020
        }
    }
}, {
    'name': 'srvDomain: the works',
    'input': {
        'srvDomain': 'chilitown',
        'cueballOptions': {
            'service': '_moray._tcp',
            'defaultPort': 2025,
            'maxDNSConcurrency': 2,
            'target': 18,
            'maximum': 25,
            'recovery': {
                'default': {
                    'retries': 17,
                    'timeout': 1234,
                    'maxTimeout': 1678,
                    'delay': 123,
                    'maxDelay': 156
                },
                'dns': {
                    'retries': 27,
                    'timeout': 2234,
                    'maxTimeout': 2678,
                    'delay': 223,
                    'maxDelay': 256
                },
                'dns_srv': {
                    'retries': 37,
                    'timeout': 3234,
                    'maxTimeout': 3678,
                    'delay': 323,
                    'maxDelay': 356
                }
            }
        }
    },
    'output': {
        'cueballOptions': {
            'domain': 'chilitown',
            'service': '_moray._tcp',
            'defaultPort': 2025,
            'maxDNSConcurrency': 2,
            'target': 18,
            'maximum': 25,
            'recovery': {
                'default': {
                    'retries': 17,
                    'timeout': 1234,
                    'maxTimeout': 1678,
                    'delay': 123,
                    'maxDelay': 156
                },
                'dns': {
                    'retries': 27,
                    'timeout': 2234,
                    'maxTimeout': 2678,
                    'delay': 223,
                    'maxDelay': 256
                },
                'dns_srv': {
                    'retries': 37,
                    'timeout': 3234,
                    'maxTimeout': 3678,
                    'delay': 323,
                    'maxDelay': 356
                }
            }
        }
    }
}, {
    'name': 'url and port specified',
    'input': { 'url': 'tcp://dental.plan:1234/', 'port': 3456 },
    'output': {
        'mode': 'direct',
        'label': 'dental.plan:3456',
        'cueballOptions': {
            'domain': 'dental.plan',
            'service': '_moraybogus._tcp',
            'defaultPort': 3456
        }
    }
}, {
    'name': 'legacy: connectTimeout option',
    'input': {
        'url': 'tcp://foobar.a.b:5678',
        'connectTimeout': 4567
    },
    'output': {
        'mode': 'direct',
        'cueballOptions': {
            'service': '_moraybogus._tcp',
            'domain': 'foobar.a.b',
            'defaultPort': 5678,
            'recovery': {
                'default': {
                    'timeout': 4567,
                    'maxTimeout': 30000,
                    'retries': 5,
                    'delay': 1000,
                    'maxDelay': 60000
                }
            }
        }
    }
}, {
    'name': 'legacy: maxConnections options (does not affect target)',
    'input': {
        'srvDomain': 'foobar.a.b',
        'maxConnections': 427
    },
    'output': {
        'cueballOptions': {
            'service': '_moray._tcp',
            'domain': 'foobar.a.b',
            'target': 6,
            'maximum': 427
        }
    }
}, {
    'name': 'legacy: maxConnections option (affects target)',
    'input': {
        'srvDomain': 'foobar.a.b',
        'maxConnections': 1
    },
    'output': {
        'cueballOptions': {
            'service': '_moray._tcp',
            'domain': 'foobar.a.b',
            'target': 1,
            'maximum': 1
        }
    }
}, {
    /* This is one of the most important default configurations. */
    'name': 'legacy: dns option (only resolvers)',
    'input': {
        'url': 'tcp://foobar.a.b:5678',
        'dns': {
            'resolvers': [ '1.2.3.4', '5.6.7.8' ]
        }
    },
    'output': {
        'cueballOptions': {
            'service': '_moraybogus._tcp',
            'domain': 'foobar.a.b',
            'defaultPort': 5678,
            'resolvers': [ '1.2.3.4', '5.6.7.8' ],
            'recovery': {
                'dns': {
                    'retries': 5,
                    'timeout': 1000,
                    'maxTimeout': 20000
                },
                'dns_srv': {
                    'retries': 0,
                    'timeout': 1,
                    'maxTimeout': 1
                }
            }
        }
    }
}, {
    'name': 'legacy: dns option (complex)',
    'input': {
        'srvDomain': 'foobar.a.b',
        'dns': {
            /* checkInterval should not appear in the output. */
            'checkInterval': 37,
            'resolvers': [ '1.2.3.4', '5.6.7.8' ],
            'timeout': 9876
        }
    },
    'output': {
        'cueballOptions': {
            'service': '_moray._tcp',
            'domain': 'foobar.a.b',
            'resolvers': [ '1.2.3.4', '5.6.7.8' ],
            'recovery': {
                'dns': {
                    'timeout': 9876,
                    'maxTimeout': 20000
                },
                'dns_srv': {
                    'timeout': 9876,
                    'maxTimeout': 20000
                }
            }
        }
    }
}, {
    'name': 'legacy: retry option',
    'input': {
        'url': 'tcp://foobar.a.b:5678',
        'retry': {
            'retries': 42,
            'minTimeout': 4567,
            'maxTimeout': 7890
        }
    },
    'output': {
        'cueballOptions': {
            'service': '_moraybogus._tcp',
            'resolvers': undefined,
            'recovery': {
                'default': {
                    'retries': 42,
                    'delay': 4567,
                    'maxDelay': 7890
                }
            }
        }
    }
}, {
    'name': 'legacy: the works',
    'input': {
        'srvDomain': 'example.com',
        'connectTimeout': 111,
        'dns': {
            /* checkInterval should not appear in the output. */
            'checkInterval': 555,
            'resolvers': [ '1.1.1.1', '2.2.2.2' ],
            'timeout': 222
        },
        'maxConnections': 333,
        'retry': {
            'retries': 444,
            'minTimeout': 777,
            'maxTimeout': 888
        }
    },
    'output': {
        'cueballOptions': {
            'domain': 'example.com',
            'service': '_moray._tcp',
            'defaultPort': 2020,
            'target': 6,
            'maximum': 333,
            'resolvers': [ '1.1.1.1', '2.2.2.2' ],
            'recovery': {
                'dns': {
                    'timeout': 222,
                    'maxTimeout': 20000
                },
                'dns_srv': {
                    'retries': 0,
                    'timeout': 222,
                    'maxTimeout': 20000
                },
                'default': {
                    'timeout': 111,
                    'maxTimeout': 30000,
                    'retries': 444,
                    'delay': 777,
                    'maxDelay': 888
                }
            }
        }
    }
} ];

main();
