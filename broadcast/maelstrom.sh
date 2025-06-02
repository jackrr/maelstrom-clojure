#!/bin/bash

set -eo pipefail

../maelstrom/maelstrom test -w broadcast --bin broadcast.clj --time-limit 20 --node-count 5 --rate 10

