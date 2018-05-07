#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2018, Joyent, Inc.
#

#
# Makefile: top-level Makefile
#
# This Makefile contains only repo-specific logic and uses included makefiles
# to supply common targets (javascriptlint, jsstyle, restdown, etc.), which are
# used by other repos as well.
#

#
# Tools
#
NPM =				npm
FAUCET =			./node_modules/.bin/faucet
CONFIGURE =			./tools/configure

#
# We use ctrun(1) to ensure that child processes created by the test cases are
# always cleaned up.  However, on systems that don't provide ctrun(1), this
# could be commented out.
#
CTRUN ?=			ctrun -o noorphan


#
# Files and other definitions
#
JSON_FILES =			package.json \
				etc/moray-test-suite-stock.json \
				etc/moray-test-suite-custom-both.json
JS_FILES :=			tools/configure \
				$(shell find lib test -name '*.js')
JSL_FILES_NODE =		$(JS_FILES)
JSSTYLE_FILES =			$(JS_FILES)
JSSTYLE_FLAGS =			-f ./tools/jsstyle.conf
JSL_CONF_NODE =			tools/jsl.node.conf

MORAY_TEST_CONFIG_FILE ?=	etc/moray-test-suite.json
MORAY_TEST_RUNDIR =		run
MORAY_TEST_ENV_FILE =		$(MORAY_TEST_RUNDIR)/env.sh

#
# Build a list of "test_<name>" targets from the contents of the "test"
# directory.  Files that match the pattern "test/<name>.test.js" will be
# executed automatically by "make test", and any individual test can be invoked
# via "make test_<name>".
#
TESTS =				$(addprefix test_,\
				    $(subst .test.js$,,\
				    $(notdir $(wildcard test/*.test.js))))

#
# Most of the test programs produce TAP output, but at least one produces
# regular bunyan output under some conditions.
#
POST_PROCESS =			$(FAUCET)
test_loop :			POST_PROCESS = bunyan -lfatal

#
# Targets
#

.PHONY: all
all:
	$(NPM) install
CLEAN_FILES += node_modules

.PHONY: test
test: $(TESTS)
	@echo tests passed

test_%: test/%.test.js | $(FAUCET) $(MORAY_TEST_ENV_FILE)
	set -o pipefail && source $(MORAY_TEST_ENV_FILE) && \
	    $(CTRUN) node $< | $(POST_PROCESS)
	@echo test $< passed

$(FAUCET): all

$(MORAY_TEST_ENV_FILE): $(MORAY_TEST_CONFIG_FILE)
	$(CONFIGURE) $^

$(MORAY_TEST_CONFIG_FILE):
	@echo
	@echo You must create $(MORAY_TEST_CONFIG_FILE) first.  See README.md.
	@exit 1

CLEAN_FILES += run

include ./Makefile.targ
