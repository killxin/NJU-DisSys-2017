#!/bin/bash
for i in `seq 100` 
do
echo $i"th test:"
go test -run Election
echo "===================="
done
