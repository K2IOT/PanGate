#!/usr/bin/env bash
# Release check (spec §2, §13): every module at the expected version, no SNAPSHOT left in build and run files, and
# every ADR listed in docs/adr/README.md with the five required headings.
# Usage: release-check.sh <expected-version>
set -uo pipefail
expected="${1:?usage: release-check.sh <expected-version>}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
fail=0
err() { echo "ERR $*"; fail=1; }

for pom in platform-parent platform-contracts platform-spring-starter gateway-devtools gateway-service auth-service; do
  f="$pom/pom.xml"
  # project <version> (a direct child of <project>, two-space indent; absent = inherited) and the internal parent's
  own=$(sed -n 's|^  <version>\(.*\)</version>.*|\1|p' "$f" | head -1)
  par=$(awk '/<parent>/{p=1} p && /<groupId>com.pangate<\/groupId>/{c=1} p && c && /<version>/{gsub(/.*<version>|<\/version>.*/,""); print; exit} /<\/parent>/{p=0}' "$f")
  [ "$own" = "$expected" ] || [ -z "$own" ] || err "$f: version $own, expected $expected"
  [ -z "$par" ] || [ "$par" = "$expected" ] || err "$f: parent version $par, expected $expected"
done

hits=$(grep -rn 'SNAPSHOT' Makefile platform-devenv .github */README.md README.md 2>/dev/null \
  | grep -v '^platform-devenv/scripts/release-check.sh:' | sort -u)
[ -z "$hits" ] || { echo "$hits"; err "SNAPSHOT left in build/run files"; }

[ -f docs/adr/README.md ] || err "docs/adr/README.md missing"
n=0
for adr in docs/adr/0*.md; do
  [ -e "$adr" ] || continue
  n=$((n + 1))
  name=$(basename "$adr")
  grep -q "$name" docs/adr/README.md 2>/dev/null || err "$name not listed in docs/adr/README.md"
  for h in 'Trạng thái' 'Bối cảnh' 'Quyết định' 'Hệ quả' 'Nguồn'; do
    grep -q "^## $h\$" "$adr" || err "$name: missing heading '## $h'"
  done
done
[ "$n" -gt 0 ] || err "no ADR in docs/adr"
# and the other way: every ADR the index links to exists
# (bare, ./ or docs/adr/ relative links all name a file in docs/adr)
for link in $(grep -oE '\(([^()]*/)?[0-9]{4}-[^()/]*\.md\)' docs/adr/README.md 2>/dev/null | tr -d '()'); do
  name=$(basename "$link")
  [ -f "docs/adr/$name" ] || err "docs/adr/README.md links to missing $link"
done

[ "$fail" -eq 0 ] && echo "release-check OK $expected"
exit "$fail"
