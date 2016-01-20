#!/bin/bash

iquery -anq "remove(zero_to_255)"                > /dev/null 2>&1

iquery -anq "store( build( <val:string> [x=0:255,10,0],  string(x % 256) ), zero_to_255 )"                       > /dev/null 2>&1


iquery -anq "store( build( <val:string> [x=0:2000000,10000,0],  string(x % 256) ), big )"                       > /dev/null 2>&1


iquery -aq "fast_count(zero_to_255)"
> /dev/null 2>&1



rm -rf /tmp/fastcount_test
mkdir /tmp/fastcount_test
touch /tmp/fastcount_test/test.expected


iquery -aq "fast_count(zero_to_255)" >> test.out

echo '255
' >> /tmp/fastcount_test/test.expected



diff test.out test.expected
