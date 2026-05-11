#!/usr/bin/env bash
set -euo pipefail

mkdir -p docs

cp README.md docs/vocabulary.md
cp csvw-safe-library/README.md docs/library.md