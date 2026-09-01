#!/usr/bin/env bash
# Point the documentation at a released version.
#
#   .github/scripts/set-documented-version.sh 0.2.3
#
# The guide, the testing page and the README all quote a version for people to
# copy. Keeping them current was a manual release step, so they sat two releases
# behind and told every reader to depend on something that was no longer current.
# The release runs this instead.
#
# Every 0.x.y in these files is a reference to this library — checked, not
# assumed — so a blanket rewrite is safe. Broker and JDK versions (3.13, 4, 17)
# do not match the pattern.
set -euo pipefail

VERSION="${1:-}"
if [ -z "$VERSION" ]; then
  echo "usage: $0 <version>   e.g. $0 0.2.3" >&2
  exit 2
fi
case "$VERSION" in
  *SNAPSHOT*)
    echo "refusing to document a snapshot: $VERSION" >&2
    exit 2
    ;;
esac

cd "$(dirname "$0")/../.."
python3 - "$VERSION" <<'PYTHON'
import pathlib, re, sys

version = sys.argv[1]
changed = []
for path in list(pathlib.Path("docs").glob("*.md")) + [pathlib.Path("README.md")]:
    if not path.exists():
        continue
    text = path.read_text()
    updated = re.sub(r"\b0\.\d+\.\d+\b", version, text)
    if updated != text:
        path.write_text(updated)
        changed.append(str(path))

print(f"documented version is now {version}")
for name in changed:
    print(f"  updated {name}")
if not changed:
    print("  nothing to change")
PYTHON
