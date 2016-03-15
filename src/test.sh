#!/bin/bash

iquery -aq "store(build(<val:double>[i=0:3,4,0,j=0:3,4,0],i*4+j),foo)" >/dev/null 2>&1
iquery -anq "remove(zero_to_255)"                > /dev/null 2>&1
iquery -anq "store( build( <val:string> [x=0:255,10,0],  string(x % 256) ), zero_to_255 )"  > /dev/null 2>&1
iquery -anq "store( build( <val:string> [x=0:255,10,5],  string(x % 256) ), zero_to_255_overlap )"  > /dev/null 2>&1

rm test.out
rm test.expected
touch ./test.expected

iquery -o csv:l -aq "aggregate(filter(summarize(zero_to_255), attid=0), sum(count) as count)" >> test.out
echo 'count' >> ./test.expected
echo '256' >> ./test.expected

iquery -o csv:l -aq "aggregate(filter(summarize(between(zero_to_255,0,9)), attid=0), sum(count) as count)" >> test.out
echo 'count' >> ./test.expected
echo '10' >> ./test.expected

iquery -o csv:l -aq "aggregate(filter(summarize(zero_to_255_overlap), attid=0), sum(count) as count)" >> test.out
echo 'count' >> ./test.expected
echo '256' >> ./test.expected

diff test.out test.expected
exit 0

