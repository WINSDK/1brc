#!/bin/bash

set -e
set -x

cargo b --release
perf record --call-graph dwarf --output /tmp/perf.data ./target/release/onebrc /home/nicolas/1brc_baseline/measurement_data.txt
perf script --input /tmp/perf.data | inferno-collapse-perf > /tmp/stacks.folded
cat /tmp/stacks.folded | inferno-flamegraph > flamegraph.svg
rm /tmp/perf.data /tmp/stacks.folded
firefox-developer-edition ./flamegraph.svg
