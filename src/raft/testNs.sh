#!/bin/bash
#testers=("TestBasicAgree" "TestFailAgree" "TestFailNoAgree" "TestConcurrentStarts" "TestRejoin" "TestBackup" "TestCount" "Persist" "TestFigure8" "TestUnreliableAgree" "TestFigure8Unreliable" "TestReliableChurn" "TestUnreliableChurn")
testers=("Agree" "TestConcurrentStarts" "TestRejoin" "TestBackup" "TestCount" "Persist" "Figure8" "Churn")
tester=${testers[0]}
for i in `seq 10`
do
go test -run $tester > /dev/null
if [[ $? == 0 ]]
then
    echo "$i th $tester Passed"
else
    echo "$i th $tester Failed"
    exit 1
fi
done