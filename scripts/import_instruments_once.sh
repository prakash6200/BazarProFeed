#!/usr/bin/env bash
set -euo pipefail

CSV_PATH="${1:-instruments.csv}"

GO111MODULE=on go run ./cmd/import_instruments_once -file "${CSV_PATH}"
