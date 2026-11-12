SHELL := /bin/bash
VERSION := 0.1.0
MVNW := $(CURDIR)/mvnw
MODULES := platform-parent platform-contracts platform-spring-starter platform-contracts-testkit gateway-devtools gateway-service push-router-service push-api-service auth-service chat-service

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
GATEWAY_JAR := gateway-service/target/gateway-service-$(VERSION)-exec.jar
DEVTOOLS_JAR := gateway-devtools/target/gateway-devtools-$(VERSION)-exec.jar

.PHONY: doctor infra-up infra-down run-local images up down logs smoke local-check memory-check load-check \
  auth-local auth-local-check router-local

## Check JDK 21, Docker, free ports and Kafka/Redis on localhost.
doctor:
	@java -version 2>&1 | grep -q 'version "21' && echo "ok  JDK 21" || echo "ERR JDK 21 required"
	@docker compose version >/dev/null 2>&1 && echo "ok  docker compose" || echo "WARN docker compose missing (needed for dev)"
	@for p in 8443 8883 9090 8081 9443; do \
	  if ss -ltn 2>/dev/null | grep -q ":$$p "; then echo "ERR port $$p in use"; else echo "ok  port $$p free"; fi; \
	done
	@for d in kafka:9092 redis:6379; do n=$${d%%:*}; p=$${d##*:}; \
	  if bash -c "</dev/tcp/127.0.0.1/$$p" 2>/dev/null; then echo "ok  $$n localhost:$$p"; \
	  else echo "WARN $$n localhost:$$p unreachable (make infra-up)"; fi; \
	done

## Local infrastructure (Kafka + Redis) on localhost for the gateway run from the IDE (profile local).
infra-up:
	$(COMPOSE) up -d --wait --wait-timeout 90 kafka redis postgres
	$(COMPOSE) up kafka-init

## ScyllaDB for chat-service (`make infra-up` does not start it).
.PHONY: scylla-up
scylla-up:
	$(COMPOSE) up -d --wait --wait-timeout 180 scylla

infra-down:
	$(COMPOSE) down -v

## Run the gateway from source with profile local (same as the IDE run configuration).
run-local:
	$(MVNW) -f gateway-service/pom.xml spring-boot:run -Dspring-boot.run.profiles=local

## Build dev images with Jib (needs a local Docker daemon).
images:
	$(MVNW) -q -f gateway-devtools/pom.xml compile jib:dockerBuild
	$(MVNW) -q -f gateway-service/pom.xml compile jib:dockerBuild
	$(MVNW) -q -f push-router-service/pom.xml compile jib:dockerBuild
	$(MVNW) -q -f push-api-service/pom.xml compile jib:dockerBuild
	$(MVNW) -q -f auth-service/pom.xml compile jib:dockerBuild

up:
	$(COMPOSE) --profile app up -d --wait --wait-timeout 90

down:
	$(COMPOSE) --profile app --profile tools down -v

logs:
	$(COMPOSE) --profile app logs -f

smoke:
	$(COMPOSE) --profile tools run --rm devtools smoke --url wss://gateway:8443/v1/ws \
	  --auth-url http://auth:8090 --client-file /dev-secrets/auth/clients.json --ca /dev-secrets/ca.crt \
	  --push-api http://push-api:8080

AUTH_JAR := auth-service/target/auth-service-$(VERSION)-exec.jar
ROUTER_JAR := push-router-service/target/push-router-service-$(VERSION)-exec.jar
PUSH_API_JAR := push-api-service/target/push-api-service-$(VERSION)-exec.jar

## push-api from source with profile local (Kafka/Redis from `make infra-up`, auth from `make auth-local`).
.PHONY: push-api-local
push-api-local:
	java -jar $(PUSH_API_JAR) --spring.profiles.active=local

## push-router from source with profile local (Kafka/Redis from `make infra-up`, a gateway from `make run-local`).
.PHONY: router-local
router-local:
	java -jar $(ROUTER_JAR) --spring.profiles.active=local

## auth-service from source with profile local (Postgres/Kafka from `make infra-up`; secrets in .dev/auth/).
auth-local:
	java -jar $(AUTH_JAR) --spring.profiles.active=local

## Start auth-service (local), wait for /ready, mint a connect token through it with devtools, stop.
auth-local-check: infra-up
	platform-devenv/scripts/auth-local-check.sh $(AUTH_JAR) $(DEVTOOLS_JAR)

local-check: infra-up
	platform-devenv/scripts/local-start-check.sh $(GATEWAY_JAR) $(DEVTOOLS_JAR)

memory-check: infra-up
	platform-devenv/scripts/idle-memory.sh $(GATEWAY_JAR) $(DEVTOOLS_JAR) 10000

## Idle-session load check; TODO: N=50000 on a machine with ulimit -n >= 120000.
N ?= 18000
load-check: infra-up
	platform-devenv/scripts/idle-load.sh $(GATEWAY_JAR) $(DEVTOOLS_JAR) $(N)

## Fuzz one target for real (spec §11: 10 min per target, maxDuration on the @FuzzTest). Findings are written
## as new inputs next to the seeds (src/test/resources/**/<Class>Inputs/) and fail the build.
FUZZ ?= FrameCodecFuzzTest
.PHONY: fuzz
fuzz:
	JAZZER_FUZZ=1 $(MVNW) -f gateway-service/pom.xml test -Dtest=$(FUZZ)

## Release check (spec §2, §13): module versions, no pre-release versions in build/run files, ADR index.
.PHONY: release-check
release-check:
	platform-devenv/scripts/release-check.sh $(VERSION)

## JMH benchmarks (spec §11): FrameCodec, PrincipalIndex, DeadlineWheel. Results in gateway-service/target/jmh.json.
JMH_ARGS ?= -f 1 -wi 3 -i 5 -rf json -rff target/jmh.json
.PHONY: bench
bench:
	$(MVNW) -q -f gateway-service/pom.xml -Pjmh test-compile exec:exec -Djmh.args="$(JMH_ARGS)"
