#!/bin/bash

set -eo pipefail

../maelstrom/maelstrom test -w unique-ids --bin uuids.clj --time-limit 5

