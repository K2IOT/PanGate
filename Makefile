SHELL := /bin/bash
MVNW := $(CURDIR)/mvnw
MODULES := platform-parent platform-contracts platform-spring-starter gateway-devtools gateway-service

.PHONY: bootstrap verify

## Build and install every module in dependency order, skipping tests.
bootstrap:
	@set -e; for m in $(MODULES); do \
	  [ -f $$m/pom.xml ] || continue; \
	  echo "==> $$m"; $(MVNW) -q -f $$m/pom.xml install -DskipTests; \
	done

## Build and install every module with unit and integration tests.
verify:
	@set -e; for m in $(MODULES); do \
	  [ -f $$m/pom.xml ] || continue; \
	  echo "==> $$m"; $(MVNW) -f $$m/pom.xml install; \
	done
