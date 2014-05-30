NODE_OPTS =
TEST_OPTS =

love:
	@echo "Feel like makin' love."

test:
	@node $(NODE_OPTS) ./node_modules/.bin/_mocha -R dot $(TEST_OPTS)

spec: 
	@node $(NODE_OPTS) ./node_modules/.bin/_mocha -R spec $(TEST_OPTS)

autotest:
	@node $(NODE_OPTS) ./node_modules/.bin/_mocha -R dot --watch $(TEST_OPTS)

autospec:
	@node $(NODE_OPTS) ./node_modules/.bin/_mocha -R spec --watch $(TEST_OPTS)
pack:
	npm pack

publish:
	npm publish

tag:
	git tag "v$$(node -e 'console.log(require("./package").version)')"

# NOTE: Sorry, mocumentation is not yet published.
doc: doc.json
	@mkdir -p doc
	@~/Documents/Mocumentation/bin/mocument \
		--type yui \
		--title Attributes \
		--priority Attributes \
		tmp/doc/data.json > doc/API.md

toc: doc.json
	@~/Documents/Mocumentation/bin/mocument \
		--type yui \
		--template toc \
		--priority Attributes \
		--var api_url=https://github.com/moll/node-attributes/blob/master/doc/API.md \
		tmp/doc/data.json > tmp/TOC.md

	echo "/^### \[Attributes\]/,/^\$$/{/^#/r tmp/TOC.md\n/^\$$/!d;}" |\
		sed -i "" -f /dev/stdin README.md

doc.json:
	@mkdir -p tmp
	@yuidoc --exclude test,node_modules --parse-only --outdir tmp/doc .

clean:
	rm -rf tmp *.tgz

createdb:
	createdb -E utf8 -T template0 assertions_test

.PHONY: love
.PHONY: test spec autotest autospec
.PHONY: pack publish tag
.PHONY: createdb
.PHONY: clean
