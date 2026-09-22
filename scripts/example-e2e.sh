#!/usr/bin/env bash
#
# Runs both examples against a real Nacos and verifies the resolver end to end:
#
#   * example/server registers a gRPC health server in Nacos under the id
#     <ip>:<port> it listens on, and serves the health service for that id only.
#   * example/client resolves the service through the grpcnacos resolver,
#     round-robins health checks over its instances and reports, per instance
#     id, how often it was answered SERVING and how often NOT_FOUND.
#
# Two servers are started, so a check for the id of one of them is answered
# NOT_FOUND whenever round-robin sent it to the other one: both ids reporting
# SERVING and NOT_FOUND proves that both instances are in the resolver's address
# list. Server B is then stopped and deregistered, and a second client run has
# to see B as never serving any more.
#
# The Nacos instance is the one from docker-compose.yml, normally started with
# `make nacos-up && make nacos-wait`. The script is invoked by `make example-e2e`
# and cleans up the processes it started, not the Nacos container.
set -euo pipefail

NACOS_ADDR="${NACOS_ADDR:-127.0.0.1:8848}"
BASE_URL="http://${NACOS_ADDR}/nacos"

GROUP="GRPCNACOS"
SERVICE="grpcnacos-e2e"
SERVER_A_PORT="${SERVER_A_PORT:-9101}"
SERVER_B_PORT="${SERVER_B_PORT:-9102}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$(mktemp -d)"
SERVER_A_PID=""
SERVER_B_PID=""

cleanup() {
	for pid in "${SERVER_A_PID}" "${SERVER_B_PID}"; do
		if [[ -n "${pid}" ]]; then
			kill -TERM "${pid}" 2>/dev/null || true
			wait "${pid}" 2>/dev/null || true
		fi
	done
	rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

log() { echo "==> $*"; }

fail() {
	echo "example-e2e: $*" >&2
	exit 1
}

# registrar_dsn <ip> <port> <dir> builds the DSN of one example server, with
# private SDK log and cache directories below DIR.
registrar_dsn() {
	echo "nacos://${NACOS_ADDR}/${SERVICE}?group=${GROUP}&ip=$1&port=$2&logDir=$3/log&cacheDir=$3/cache&logLevel=error&notLoadCacheAtStart=true"
}

# resolver_dsn <dir> builds the resolver DSN, which doubles as the gRPC target.
resolver_dsn() {
	echo "nacos://${NACOS_ADDR}/${SERVICE}?group=${GROUP}&logDir=$1/log&cacheDir=$1/cache&logLevel=error&notLoadCacheAtStart=true"
}

require_ready() {
	curl -fsS "${BASE_URL}/v1/console/health/readiness" >/dev/null 2>&1 ||
		fail "Nacos at ${NACOS_ADDR} is not ready, run 'make nacos-up && make nacos-wait' first"
}

# require_instance <port> <want>: polls the instance list of ${SERVICE} until an
# instance on 127.0.0.1:<port> shows up (want=1) or is gone (want=0).
#
# This uses the list endpoint: the payload Nacos 2.5.2 answers with carries a
# "port" field per instance, so presence is detected by grepping for it.
require_instance() {
	local port="$1" want="$2" present=0
	for _ in $(seq 1 60); do
		if curl -fsS "${BASE_URL}/v1/ns/instance/list?serviceName=${SERVICE}&groupName=${GROUP}" 2>/dev/null |
			grep -Eq "\"port\"[[:space:]]*:[[:space:]]*${port}"; then
			present=1
		else
			present=0
		fi
		if [[ "${present}" == "${want}" ]]; then
			return 0
		fi
		sleep 0.5
	done

	fail "instance 127.0.0.1:${port} of service ${SERVICE} is present=${present}, want ${want}"
}

# any_server_gone reports whether one of the servers this script started exited.
any_server_gone() {
	local pid
	for pid in "${SERVER_A_PID}" "${SERVER_B_PID}"; do
		if [[ -n "${pid}" ]] && ! kill -0 "${pid}" 2>/dev/null; then
			return 0
		fi
	done

	return 1
}

# wait_for_output <file> <needle> <attempts> <what>
wait_for_output() {
	local file="$1" needle="$2" attempts="$3" what="$4"

	for _ in $(seq 1 "${attempts}"); do
		if grep -qF "${needle}" "${file}"; then
			return 0
		fi
		if any_server_gone; then
			break
		fi
		sleep 0.5
	done

	echo "--- captured output ---" >&2
	cat "${file}" >&2
	echo "-----------------------" >&2
	fail "${what}"
}

require_ready

log "building the examples"
(cd "${REPO_ROOT}" && go build -o "${WORK_DIR}/example-server" ./example/server)
(cd "${REPO_ROOT}" && go build -o "${WORK_DIR}/example-client" ./example/client)

log "starting example/server A on 127.0.0.1:${SERVER_A_PORT}"
SERVER_A_OUT="${WORK_DIR}/server-a.out"
GRPCNACOS_DSN="$(registrar_dsn 127.0.0.1 "${SERVER_A_PORT}" "${WORK_DIR}/server-a")" \
	"${WORK_DIR}/example-server" >"${SERVER_A_OUT}" 2>&1 &
SERVER_A_PID=$!
wait_for_output "${SERVER_A_OUT}" "listening 127.0.0.1:${SERVER_A_PORT}" 120 \
	"example/server A did not start listening"
cat "${SERVER_A_OUT}"

log "starting example/server B on 127.0.0.1:${SERVER_B_PORT}"
SERVER_B_OUT="${WORK_DIR}/server-b.out"
GRPCNACOS_DSN="$(registrar_dsn 127.0.0.1 "${SERVER_B_PORT}" "${WORK_DIR}/server-b")" \
	"${WORK_DIR}/example-server" >"${SERVER_B_OUT}" 2>&1 &
SERVER_B_PID=$!
wait_for_output "${SERVER_B_OUT}" "listening 127.0.0.1:${SERVER_B_PORT}" 120 \
	"example/server B did not start listening"
cat "${SERVER_B_OUT}"

log "waiting for both instances to show up in Nacos"
require_instance "${SERVER_A_PORT}" 1
require_instance "${SERVER_B_PORT}" 1

log "running example/client with both instances registered"
CLIENT_BOTH_OUT="${WORK_DIR}/client-both.out"
if ! GRPCNACOS_DSN="$(resolver_dsn "${WORK_DIR}/client-both")" \
	GRPCNACOS_INSTANCES="127.0.0.1:${SERVER_A_PORT},127.0.0.1:${SERVER_B_PORT}" \
	"${WORK_DIR}/example-client" >"${CLIENT_BOTH_OUT}" 2>&1; then
	cat "${CLIENT_BOTH_OUT}" >&2
	fail "example/client exited with an error"
fi
cat "${CLIENT_BOTH_OUT}"
grep -qF "round-robin ok" "${CLIENT_BOTH_OUT}" ||
	fail "example/client did not round-robin over both instances"
log "example/client round-robined over both instances"

log "stopping example/server B"
kill -TERM "${SERVER_B_PID}"
wait_for_output "${SERVER_B_OUT}" "stopping" 120 "example/server B did not shut down gracefully"
wait "${SERVER_B_PID}" || true
SERVER_B_PID=""

require_instance "${SERVER_B_PORT}" 0
log "example/server B is gone from Nacos"

log "running example/client with only instance A registered"
CLIENT_A_ONLY_OUT="${WORK_DIR}/client-a-only.out"
if ! GRPCNACOS_DSN="$(resolver_dsn "${WORK_DIR}/client-a-only")" \
	GRPCNACOS_INSTANCES="127.0.0.1:${SERVER_A_PORT},127.0.0.1:${SERVER_B_PORT}" \
	"${WORK_DIR}/example-client" >"${CLIENT_A_ONLY_OUT}" 2>&1; then
	cat "${CLIENT_A_ONLY_OUT}" >&2
	fail "example/client exited with an error"
fi
cat "${CLIENT_A_ONLY_OUT}"
grep -qF "instance 127.0.0.1:${SERVER_B_PORT} serving=0" "${CLIENT_A_ONLY_OUT}" ||
	fail "example/client still reached instance B after it was deregistered"
grep -qE "instance 127.0.0.1:${SERVER_A_PORT} serving=[1-9]" "${CLIENT_A_ONLY_OUT}" ||
	fail "example/client did not reach instance A any more"
log "example/client saw the deregistered instance disappear"

log "stopping example/server A"
kill -TERM "${SERVER_A_PID}"
wait_for_output "${SERVER_A_OUT}" "stopping" 120 "example/server A did not shut down gracefully"
wait "${SERVER_A_PID}" || true
SERVER_A_PID=""

log "example end-to-end verification passed"
