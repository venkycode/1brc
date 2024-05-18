#! /bin/zsh

# Compile the code
go build -o main

time ./main -input '../../../../measurements.txt'