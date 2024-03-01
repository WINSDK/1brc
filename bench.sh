#!/bin/bash

set -e
set -x

cargo b --release
perf record --call-graph dwarf --output /tmp/perf.data ./target/release/onebrc /home/nicolas/1brc_baseline/measurement_data.txt > /dev/null
perf script --input /tmp/perf.data > /tmp/perf.script
cat /tmp/perf.script | inferno-collapse-perf > /tmp/stacks.folded
cat /tmp/stacks.folded | inferno-flamegraph > flamegraph.svg
firefox-developer-edition ./flamegraph.svg
