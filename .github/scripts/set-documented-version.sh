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
# What it rewrites, and why that is a smaller set than it used to be
# ------------------------------------------------------------------
# Only versions a reader would **copy**: inside a fenced code block, inside an
# inline `code span`, or in a shields.io badge URL. Prose is never touched.
#
# This used to rewrite every three-segment 0.x.y in these files, and that is a
# different thing from rewriting the coordinates. A sentence about a *past*
# release is not a version to copy, and rewriting one turns a true statement
# into a false one: "since 0.5.0 all five write both" silently became a claim
# about whatever was being released. Three of those accumulated by 0.6.0.
#
# The workaround was a convention -- write history with two segments, `0.5`,
# which the pattern did not match -- and a convention is a thing somebody has to
# remember at exactly the moment they are thinking about something else. It came
# within one word of failing again while 0.7.2 was being written.
#
# So the rule is structural now. A coordinate lives in code; a claim about the
# past lives in prose; and the script can tell them apart without anybody
# remembering anything. Two-segment history still reads well and is still worth
# writing, but it is no longer load-bearing.
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
VERSION = re.compile(r"\b0\.\d+\.\d+\b")
FENCE = re.compile(r"^\s*```")
# A span of `backticked text`, and a shields.io badge URL. Both are things a
# reader copies or a renderer shows as the current version.
SPAN = re.compile(r"`[^`\n]*`")
BADGE = re.compile(r"https://img\.shields\.io/\S*")

changed, rewritten, left = [], 0, 0

for path in list(pathlib.Path("docs").glob("*.md")) + [pathlib.Path("README.md")]:
    if not path.exists():
        continue

    out, inside_fence = [], False
    for line in path.read_text().splitlines(keepends=True):
        if FENCE.match(line):
            inside_fence = not inside_fence
            out.append(line)
            continue

        if inside_fence:
            new, n = VERSION.subn(version, line)
            rewritten += n
            out.append(new)
            continue

        # Outside a fence only the coordinate-shaped parts are touched, so a
        # sentence mentioning a past release survives intact.
        def replace_within(match):
            global rewritten
            new, n = VERSION.subn(version, match.group(0))
            rewritten += n
            return new

        line = BADGE.sub(replace_within, line)
        line = SPAN.sub(replace_within, line)
        # Counted with the coordinate-shaped parts removed, because the version
        # just written in matches the same pattern -- counting the whole line
        # would report every coordinate it rewrote as one it left alone.
        left += len(VERSION.findall(BADGE.sub("", SPAN.sub("", line))))
        out.append(line)

    updated = "".join(out)
    if updated != path.read_text():
        path.write_text(updated)
        changed.append(str(path))

print(f"documented version is now {version}")
for name in changed:
    print(f"  updated {name}")
if not changed:
    print("  nothing to change")
print(f"  {rewritten} coordinate(s) rewritten, {left} version(s) left alone in prose")
PYTHON
