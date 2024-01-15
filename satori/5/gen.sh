#!/bin/bash

n=$1
# RANDOM=30345
RANDOM=$(echo $RANDOM | tee stderr.out)

echo $n
echo 18446744073709551615
echo 18446744073709551615


for (( i=0; i<$n; i++ ))
do
   echo $((RANDOM%2))
   # echo 1
done
