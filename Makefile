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

COMPOSE := docker compose -f platform-devenv/docker-compose.yml
GATEWAY_JAR := gateway-service/target/gateway-service-0.1.0-SNAPSHOT.jar
DEVTOOLS_JAR := gateway-devtools/target/gateway-devtools-0.1.0-SNAPSHOT-exec.jar

.PHONY: doctor run-local images up down logs smoke local-check memory-check

## Check JDK 21, Docker and free ports.
doctor:
	@java -version 2>&1 | grep -q 'version "21' && echo "ok  JDK 21" || echo "ERR JDK 21 required"
	@docker compose version >/dev/null 2>&1 && echo "ok  docker compose" || echo "WARN docker compose missing (needed for dev)"
	@for p in 8443 8883 8081; do \
	  if ss -ltn 2>/dev/null | grep -q ":$$p "; then echo "ERR port $$p in use"; else echo "ok  port $$p free"; fi; \
	done

## Run the gateway from source with profile local (same as the IDE run configuration).
run-local:
	$(MVNW) -f gateway-service/pom.xml spring-boot:run -Dspring-boot.run.profiles=local

## Build dev images with Jib (needs a local Docker daemon).
images:
	$(MVNW) -q -f gateway-devtools/pom.xml jib:dockerBuild
	$(MVNW) -q -f gateway-service/pom.xml jib:dockerBuild

up:
	$(COMPOSE) --profile app up -d --wait --wait-timeout 90

down:
	$(COMPOSE) --profile app --profile tools down -v

logs:
	$(COMPOSE) --profile app logs -f

smoke:
	$(COMPOSE) --profile tools run --rm devtools smoke --url wss://gateway:8443/v1/ws \
	  --issuer http://issuer:8090 --ca /dev-secrets/ca.crt

local-check:
	platform-devenv/scripts/local-start-check.sh $(GATEWAY_JAR) $(DEVTOOLS_JAR)

memory-check:
	platform-devenv/scripts/idle-memory.sh $(GATEWAY_JAR) $(DEVTOOLS_JAR) 10000
