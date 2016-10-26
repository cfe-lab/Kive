#!/bin/bash
        cat "$1" | cut -d ',' -f 2 | tr 'ATCG' 'TAGC' | paste -d, "$1" - | cut -d ',' -f 1,3 > "$2"
        