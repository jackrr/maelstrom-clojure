#!/bin/bash

set -eo pipefail

../maelstrom/maelstrom test -w echo --bin echo.clj --time-limit 5
