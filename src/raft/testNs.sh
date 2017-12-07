#!/bin/bash
testers=("Agree" "TestConcurrentStarts" "TestRejoin" "TestBackup" "TestCount" "Persist" "Figure8" "Churn" "Election")
for n in `seq ${#testers[*]}`
do
tester=${testers[n-1]}
#tester="^TestFigure8$"
echo $tester
for i in `seq 10`
do
go test -run $tester > /dev/null
if [[ $? == 0 ]]
then
    echo "$i th Passed"
else
    echo "$i th Failed"
    exit 1
fi
done
echo "================"
done