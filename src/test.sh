#!/bin/bash

iquery -anq "remove(zero_to_255)"                > /dev/null 2>&1

iquery -anq "store( build( <val:string> [x=0:255,10,0],  string(x % 256) ), zero_to_255 )"                       > /dev/null 2>&1


iquery -anq "store( build( <val:string> [x=0:2000000,10000,0],  string(x % 256) ), big )"                       > /dev/null 2>&1


rm test.out
rm test.expected

touch ./test.expected


iquery -o csv -aq "fast_count(zero_to_255)" >> test.out

echo 'count' >> ./test.expected
echo '256
' >> ./test.expected



diff test.out test.expected
