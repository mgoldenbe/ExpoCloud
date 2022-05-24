#!/bin/sh
freq=${2:-10} # either $2 or 10 by default.
awk -v freq=$freq -v title="$(head -n 1 $1)" -v nlines=$(wc -l < $1) \
       '{print;} NR>1 && NR<nlines && (NR-1) % freq == 0 { print ""; print title}' $1 | \
        column -s, -t -e
