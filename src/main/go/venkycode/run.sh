#! /bin/zsh
set -e
# Compile the code
go build -o main

output_file="output.txt"
# if first arg is "profile"
if [ $1 = "profile" ]; then
    echo "Profiling the code"
    time ./main -input '../../../../measurements.txt' -profile > $output_file
else
    echo "Running the code"
    time ./main -input '../../../../measurements.txt' > $output_file
fi


correct_output_file="./correct_output.txt"

# Check if the output is correct
if diff $output_file $correct_output_file; then
    echo "Output is correct"
else
    echo "Output is incorrect"
fi