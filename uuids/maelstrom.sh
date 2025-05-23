#!/bin/bash

set -eo pipefail

../maelstrom/maelstrom test -w uuid --bin uuids.clj --time-limit 5
