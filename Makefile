DBPATH := test/db/mongo

test: $(DBPATH) install-mongo
		@LOG=test* ./node_modules/.bin/prok \
		--env test/env.test \
		--procfile test/Procfile.test \
		--root .

install:
	@npm install

$(DBPATH):
	@mkdir -p $@

install-mongo:
ifeq (,$(shell which mongod))
	@echo installing mongodb...
	@brew install mongodb
else
endif

.PHONY: test
