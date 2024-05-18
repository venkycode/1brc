#! /bin/zsh

# Compile the code
go build -o main

# if first arg is "profile"
if [ $1 = "profile" ]; then
    echo "Profiling the code"
    time ./main -input '../../../../measurements.txt' -profile
else
    echo "Running the code"
    time ./main -input '../../../../measurements.txt'
fi


