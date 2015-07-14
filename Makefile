DBPATH := test/db/mongo

test: node_modules_test $(DBPATH) install-mongo
		@LOG=test* ./node_modules/.bin/prok \
		--env test/env.test \
		--procfile test/Procfile.test \
		--root .

node_modules_test: package.json
	@npm install --dev
	@touch node_modules

$(DBPATH):
	@mkdir -p $@

install-mongo:
ifeq (,$(shell which mongod))
	@echo installing mongodb...
	@brew install mongodb
else
endif

.PHONY: test
