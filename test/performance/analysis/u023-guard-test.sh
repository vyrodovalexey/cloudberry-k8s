#!/usr/bin/env bash
# Truth-table verification of the campaign resume guard BEFORE any launch.
# skip_block NEXT is true (skip) iff NEXT <= START_FROM.
# Block N runs iff skip_block (N+1) is false, i.e. iff (N+1) > START_FROM.
skip_block() { [[ ! "$1" > "${START_FROM}" ]]; }
fails=0
check() { # $1=START_FROM $2=block $3=expected(runs=1/skips=0)
  local START_FROM="$1" b="$2" exp="$3"
  local next
  case "$b" in
    PTC-220-01) next=PTC-220-02;; PTC-220-02) next=PTC-220-03;; PTC-220-03) next=PTC-220-04;;
    PTC-220-04) next=PTC-220-05;; PTC-220-05) next=PTC-220-06;; PTC-220-06) next=PTC-220-07;;
    PTC-220-07) next=PTC-220-08;; PTC-220-08) next=PTC-220-09;; PTC-220-09) next=PTC-220-10;;
    PTC-220-10) next=PTC-220-11;; PTC-220-11) next=PTC-220-12;; PTC-220-12) next=PTC-220-13;;
    PTC-220-13) next=PTC-220-14;; PTC-220-14) next=PTC-220-15;; PTC-220-15) next=PTC-220-16;;
    PTC-220-16) next=PTC-220-17;; PTC-220-17) next=PTC-220-18;;
  esac
  local runs=1
  skip_block "$next" && runs=0     # guard true => body skipped
  [[ "$runs" == "$exp" ]] || { echo "FAIL START_FROM=$START_FROM block=$b expected_runs=$exp got=$runs"; fails=$((fails+1)); }
}
# full run: everything runs
for b in $(seq -f "PTC-220-%02g" 1 17); do check PTC-220-01 "$b" 1; done
# resume at 03: 01,02 skip; 03..17 run
for b in 01 02; do check PTC-220-03 "PTC-220-$b" 0; done
for n in $(seq 3 17); do check PTC-220-03 "$(printf 'PTC-220-%02d' $n)" 1; done
# resume at 07: only 07..17 run
for n in $(seq 1 6); do check PTC-220-07 "$(printf 'PTC-220-%02d' $n)" 0; done
for n in $(seq 7 17); do check PTC-220-07 "$(printf 'PTC-220-%02d' $n)" 1; done
[[ $fails -eq 0 ]] && echo "GUARD TRUTH TABLE: ALL PASS" || { echo "$fails FAILURES"; exit 1; }
