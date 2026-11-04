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

.PHONY: doctor infra-up infra-down run-local images up down logs smoke local-check memory-check load-check

## Check JDK 21, Docker, free ports and Kafka/Redis on localhost.
doctor:
	@java -version 2>&1 | grep -q 'version "21' && echo "ok  JDK 21" || echo "ERR JDK 21 required"
	@docker compose version >/dev/null 2>&1 && echo "ok  docker compose" || echo "WARN docker compose missing (needed for dev)"
	@for p in 8443 8883 9090 8081; do \
	  if ss -ltn 2>/dev/null | grep -q ":$$p "; then echo "ERR port $$p in use"; else echo "ok  port $$p free"; fi; \
	done
	@for d in kafka:9092 redis:6379; do n=$${d%%:*}; p=$${d##*:}; \
	  if bash -c "</dev/tcp/127.0.0.1/$$p" 2>/dev/null; then echo "ok  $$n localhost:$$p"; \
	  else echo "WARN $$n localhost:$$p unreachable (make infra-up)"; fi; \
	done

## Local infrastructure (Kafka + Redis) on localhost for the gateway run from the IDE (profile local).
infra-up:
	$(COMPOSE) up -d --wait --wait-timeout 90 kafka redis
	$(COMPOSE) up kafka-init

infra-down:
	$(COMPOSE) down -v

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
	  --issuer http://issuer:8090 --ca /dev-secrets/ca.crt --router gateway:9090

local-check: infra-up
	platform-devenv/scripts/local-start-check.sh $(GATEWAY_JAR) $(DEVTOOLS_JAR)

memory-check: infra-up
	platform-devenv/scripts/idle-memory.sh $(GATEWAY_JAR) $(DEVTOOLS_JAR) 10000

## Idle-session load check; TODO: N=50000 on a machine with ulimit -n >= 120000.
N ?= 18000
load-check: infra-up
	platform-devenv/scripts/idle-load.sh $(GATEWAY_JAR) $(DEVTOOLS_JAR) $(N)
