#!/usr/bin/env bash
#
# Renders the Mermaid diagrams in README.md to PNG.
#
# The diagrams live in the README as Mermaid source, which GitHub renders
# natively — that is the copy which is always correct, because it cannot drift
# from what is committed. These PNGs exist for the places that cannot render
# Mermaid: slides, a CV, a PDF.
#
# Generated rather than drawn, and regenerated rather than edited. A diagram
# maintained by hand alongside a source-of-truth diagram is a diagram that is
# quietly wrong within a month.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${OUT:-$ROOT/docs/images}"
WIDTH="${WIDTH:-1400}"

NAMES=(architecture core-substitution operations)

command -v npx >/dev/null 2>&1 || {
	echo "render-diagrams: npx is required (Node.js)" >&2
	exit 2
}

mkdir -p "$OUT"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

python3 - "$ROOT/README.md" "$work" <<'PY'
import re, sys, pathlib
readme, work = sys.argv[1], pathlib.Path(sys.argv[2])
blocks = re.findall(r'```mermaid\n(.*?)```', pathlib.Path(readme).read_text(encoding='utf-8'), re.S)
for i, b in enumerate(blocks):
    (work / f'{i}.mmd').write_text(b, encoding='utf-8')
print(len(blocks))
PY

count=$(ls "$work"/*.mmd 2>/dev/null | wc -l | tr -d ' ')
if [[ "$count" -ne "${#NAMES[@]}" ]]; then
	echo "render-diagrams: found $count diagrams but have ${#NAMES[@]} names" >&2
	echo "  add a name to NAMES in this script for each new diagram" >&2
	exit 1
fi

for i in "${!NAMES[@]}"; do
	name="${NAMES[$i]}"
	echo "rendering $name"
	npx --yes @mermaid-js/mermaid-cli \
		-i "$work/$i.mmd" -o "$OUT/$name.png" \
		-w "$WIDTH" -b white >/dev/null
done

echo "written to $OUT"
ls -1 "$OUT"
